package cl.duoc.analitica.messaging.consumer;

import cl.duoc.analitica.messaging.event.PedidoCreadoEvent;
import cl.duoc.analitica.messaging.producer.AlertaPublisher;
import cl.duoc.analitica.service.AnalisisService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumidor Kafka: lee pedidos-creados y delega la logica al servicio. */
@Component
public class PedidoCreadoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoCreadoConsumer.class);

    private final AnalisisService analisisService;
    private final AlertaPublisher alertaPublisher;

    public PedidoCreadoConsumer(AnalisisService analisisService, AlertaPublisher alertaPublisher) {
        this.analisisService = analisisService;
        this.alertaPublisher = alertaPublisher;
    }

    // concurrency = 3: un hilo por particion del topico
    @KafkaListener(topics = "pedidos-creados", groupId = "analitica-group", concurrency = "3")
    public void consumir(ConsumerRecord<String, PedidoCreadoEvent> record) {
        log.info("Recibido pedido de key={} particion={} offset={}",
                record.key(), record.partition(), record.offset());
        analisisService.evaluar(record.value()).ifPresent(alertaPublisher::publicar);
    }
}