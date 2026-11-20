package cl.duoc.pedidos360_backend.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declara los topicos de Kafka; se crean al arrancar si no existen. */
@Configuration
public class KafkaConfig {

    public static final String TOPIC_PEDIDOS_CREADOS = "pedidos-creados";
    public static final String TOPIC_PEDIDOS_ALERTAS = "pedidos-alertas";

    @Bean
    public NewTopic pedidosCreados() {
        return TopicBuilder.name(TOPIC_PEDIDOS_CREADOS)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 dias
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    @Bean
    public NewTopic pedidosAlertas() {
        return TopicBuilder.name(TOPIC_PEDIDOS_ALERTAS)
                .partitions(1)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000") // 30 dias
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}