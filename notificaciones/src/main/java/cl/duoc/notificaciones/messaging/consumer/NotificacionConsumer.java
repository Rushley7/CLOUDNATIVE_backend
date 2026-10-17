package cl.duoc.notificaciones.messaging.consumer;

import cl.duoc.notificaciones.exception.EventoInvalidoException;
import cl.duoc.notificaciones.messaging.event.PedidoCreadoEvent;
import cl.duoc.notificaciones.service.NotificacionService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor de la cola de notificaciones.
 *  - Éxito:                ACK.
 *  - Error no recuperable: NACK sin requeue -> DLX -> DLQ.
 *  - Error recuperable:    un reintento (NACK con requeue); si vuelve a fallar -> DLQ.
 */
@Component
public class NotificacionConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificacionConsumer.class);

    private final NotificacionService service;

    public NotificacionConsumer(NotificacionService service) {
        this.service = service;
    }

    @RabbitListener(queues = "${app.rabbit.queue}")
    public void procesar(PedidoCreadoEvent evento,
                         Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                         @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered)
            throws IOException {
        try {
            service.enviar(evento);
            channel.basicAck(tag, false);
        } catch (EventoInvalidoException e) {
            log.error("Mensaje invalido, se envia a la DLQ: {}", e.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception e) {
            if (Boolean.TRUE.equals(redelivered)) {
                log.error("Fallo definitivo tras reintento, se envia a la DLQ: {}", e.getMessage());
                channel.basicNack(tag, false, false);
            } else {
                log.warn("Fallo temporal, se reintenta una vez: {}", e.getMessage());
                channel.basicNack(tag, false, true);
            }
        }
    }
}
