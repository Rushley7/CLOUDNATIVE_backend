package cl.duoc.pedidos360_backend.messaging.producer;

import cl.duoc.pedidos360_backend.config.KafkaConfig;
import cl.duoc.pedidos360_backend.messaging.event.PedidoCreadoEvent;
import cl.duoc.pedidos360_backend.model.Pedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Productor Kafka: publica el pedido creado en el topico pedidos-creados. */
@Component
public class PedidoKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PedidoKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publicarPedidoCreado(Pedido pedido) {
        PedidoCreadoEvent evento = new PedidoCreadoEvent(
                pedido.getId(),
                pedido.getCliente(),
                pedido.getProducto(),
                pedido.getCantidad(),
                pedido.getPrecio(),
                pedido.getEstado(),
                pedido.getFechaCreacion());
        try {
            // la key es el cliente: sus pedidos quedan en la misma particion (orden garantizado)
            kafkaTemplate.send(KafkaConfig.TOPIC_PEDIDOS_CREADOS, pedido.getCliente(), evento)
                    .whenComplete((resultado, error) -> {
                        if (error != null) {
                            log.error("Kafka: fallo al publicar pedido id={}: {}", pedido.getId(), error.getMessage());
                        } else {
                            log.info("Kafka: pedido id={} publicado en particion={} offset={}",
                                    pedido.getId(),
                                    resultado.getRecordMetadata().partition(),
                                    resultado.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("Kafka: no se pudo publicar el pedido id={}: {}", pedido.getId(), e.getMessage());
        }
    }
}