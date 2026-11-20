package cl.duoc.analitica.messaging.producer;

import cl.duoc.analitica.messaging.event.AlertaPedidoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Productor Kafka: publica alertas en pedidos-alertas. */
@Component
public class AlertaPublisher {

    public static final String TOPIC_ALERTAS = "pedidos-alertas";

    private static final Logger log = LoggerFactory.getLogger(AlertaPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AlertaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publicar(AlertaPedidoEvent alerta) {
        kafkaTemplate.send(TOPIC_ALERTAS, alerta.cliente(), alerta)
                .whenComplete((resultado, error) -> {
                    if (error != null) {
                        log.error("Fallo al publicar alerta del pedido {}: {}", alerta.pedidoId(), error.getMessage());
                    } else {
                        log.info("Alerta del pedido {} publicada en particion={} offset={}",
                                alerta.pedidoId(),
                                resultado.getRecordMetadata().partition(),
                                resultado.getRecordMetadata().offset());
                    }
                });
    }
}