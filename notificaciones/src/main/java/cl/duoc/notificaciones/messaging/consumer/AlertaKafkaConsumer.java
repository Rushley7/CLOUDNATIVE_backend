package cl.duoc.notificaciones.messaging.consumer;

import cl.duoc.notificaciones.messaging.event.AlertaPedidoEvent;
import cl.duoc.notificaciones.service.AlertaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumidor Kafka: lee pedidos-alertas con su propio consumer group. */
@Component
public class AlertaKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertaKafkaConsumer.class);

    private final AlertaService alertaService;

    public AlertaKafkaConsumer(AlertaService alertaService) {
        this.alertaService = alertaService;
    }

    @KafkaListener(topics = "pedidos-alertas", groupId = "notificaciones-alertas-group")
    public void consumir(ConsumerRecord<String, AlertaPedidoEvent> record) {
        log.info("Kafka: alerta recibida key={} particion={} offset={}",
                record.key(), record.partition(), record.offset());
        alertaService.notificar(record.value());
    }
}