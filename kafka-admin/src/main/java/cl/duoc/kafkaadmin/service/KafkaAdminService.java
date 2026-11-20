package cl.duoc.kafkaadmin.service;

import cl.duoc.kafkaadmin.dto.PartitionsRequest;
import cl.duoc.kafkaadmin.dto.TopicConfigRequest;
import cl.duoc.kafkaadmin.dto.TopicRequest;
import cl.duoc.kafkaadmin.exception.ConflictoException;
import cl.duoc.kafkaadmin.exception.NoEncontradoException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Administra topicos y consumer groups usando el AdminClient de Kafka. */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private static final Set<String> POLITICAS = Set.of("delete", "compact", "compact,delete");

    private final Admin admin;

    public KafkaAdminService(Admin admin) {
        this.admin = admin;
    }

    // ---------- topicos ----------

    public List<String> listarTopicos() {
        List<String> nombres = new ArrayList<>(esperar(admin.listTopics().names()));
        Collections.sort(nombres);
        return nombres;
    }

    public Map<String, Object> detalleTopico(String name) {
        var td = esperar(admin.describeTopics(List.of(name)).allTopicNames()).get(name);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all()).get(recurso);

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("name", name);
        r.put("partitions", td.partitions().size());
        r.put("replicationFactor", td.partitions().get(0).replicas().size());
        r.put("retentionMs", config.get(TopicConfig.RETENTION_MS_CONFIG).value());
        r.put("cleanupPolicy", config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
        return r;
    }

    public void crearTopico(TopicRequest req) {
        if (req == null || req.name() == null || !req.name().matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("El nombre solo admite letras, numeros, punto, guion y guion bajo");
        }
        int particiones = req.partitions() == null ? 1 : req.partitions();
        short replicas = req.replicationFactor() == null ? 1 : req.replicationFactor();
        if (particiones < 1 || replicas < 1) {
            throw new IllegalArgumentException("Particiones y replicas deben ser mayores a 0");
        }
        Map<String, String> configs = new HashMap<>();
        if (req.retentionMs() != null) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(req.retentionMs()));
        }
        if (req.cleanupPolicy() != null) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, validarPolitica(req.cleanupPolicy()));
        }
        esperar(admin.createTopics(List.of(new NewTopic(req.name(), particiones, replicas).configs(configs))).all());
        log.info("Topico creado: {}", req.name());
    }

    public void modificarConfig(String name, TopicConfigRequest req) {
        List<AlterConfigOp> cambios = new ArrayList<>();
        if (req.retentionMs() != null) {
            cambios.add(set(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(req.retentionMs())));
        }
        if (req.cleanupPolicy() != null) {
            cambios.add(set(TopicConfig.CLEANUP_POLICY_CONFIG, validarPolitica(req.cleanupPolicy())));
        }
        if (cambios.isEmpty()) {
            throw new IllegalArgumentException("Envia retentionMs o cleanupPolicy");
        }
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, name);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, cambios)).all());
        log.info("Configuracion modificada en el topico {}", name);
    }

    public void aumentarParticiones(String name, PartitionsRequest req) {
        if (req == null || req.partitions() == null || req.partitions() < 1) {
            throw new IllegalArgumentException("Envia partitions con un numero mayor a 0");
        }
        esperar(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(req.partitions()))).all());
        log.info("Topico {} ahora con {} particiones", name, req.partitions());
    }

    public void eliminarTopico(String name) {
        esperar(admin.deleteTopics(List.of(name)).all());
        log.info("Topico eliminado: {}", name);
    }

    // ---------- consumer groups ----------

    public List<String> listarGrupos() {
        List<String> grupos = new ArrayList<>();
        esperar(admin.listConsumerGroups().all()).forEach(g -> grupos.add(g.groupId()));
        Collections.sort(grupos);
        return grupos;
    }

    public Map<String, Object> detalleGrupo(String id) {
        ConsumerGroupDescription desc = esperar(admin.describeConsumerGroups(List.of(id)).all()).get(id);
        Map<TopicPartition, OffsetAndMetadata> offsets =
                esperar(admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata());
        if (desc.members().isEmpty() && offsets.isEmpty()) {
            throw new NoEncontradoException("No existe el consumer group: " + id);
        }

        Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
        offsets.keySet().forEach(tp -> pedido.put(tp, OffsetSpec.latest()));
        var finales = pedido.isEmpty() ? Map.<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>of()
                : esperar(admin.listOffsets(pedido).all());

        List<Map<String, Object>> filas = new ArrayList<>();
        offsets.forEach((tp, om) -> {
            long fin = finales.get(tp).offset();
            Map<String, Object> fila = new LinkedHashMap<>();
            fila.put("topic", tp.topic());
            fila.put("partition", tp.partition());
            fila.put("offset", om.offset());
            fila.put("logEnd", fin);
            fila.put("lag", fin - om.offset());
            filas.add(fila);
        });

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("group", id);
        r.put("members", desc.members().size());
        r.put("offsets", filas);
        return r;
    }

    // ---------- utilidades ----------

    private AlterConfigOp set(String clave, String valor) {
        return new AlterConfigOp(new ConfigEntry(clave, valor), AlterConfigOp.OpType.SET);
    }

    private String validarPolitica(String politica) {
        if (!POLITICAS.contains(politica)) {
            throw new IllegalArgumentException("cleanupPolicy debe ser delete, compact o compact,delete");
        }
        return politica;
    }

    /** Espera el resultado de Kafka y traduce sus errores a excepciones propias. */
    private <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida");
        } catch (TimeoutException e) {
            throw new IllegalStateException("Kafka no responde (timeout)");
        } catch (ExecutionException e) {
            Throwable causa = e.getCause();
            String msg = causa.getMessage();
            if (causa instanceof UnknownTopicOrPartitionException) {
                throw new NoEncontradoException("No existe el topico");
            }
            if (causa instanceof TopicExistsException) {
                throw new ConflictoException("El topico ya existe");
            }
            if (causa instanceof InvalidRequestException || causa instanceof InvalidTopicException
                    || causa instanceof InvalidPartitionsException || causa instanceof InvalidConfigurationException
                    || causa instanceof InvalidReplicationFactorException) {
                throw new IllegalArgumentException(msg);
            }
            throw new IllegalStateException("Error de Kafka: " + msg);
        }
    }
}