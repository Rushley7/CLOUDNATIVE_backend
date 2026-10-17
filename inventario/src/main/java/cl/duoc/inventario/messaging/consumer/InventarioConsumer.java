package cl.duoc.inventario.messaging.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.inventario.exception.EventoInvalidoException;
import cl.duoc.inventario.messaging.event.PedidoCreadoEvent;
import cl.duoc.inventario.service.InventarioService;

@Component
public class InventarioConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventarioConsumer.class);

    private final InventarioService service;

    public InventarioConsumer(InventarioService service) {
        this.service = service;
    }

    @RabbitListener(queues = "${app.rabbit.queue}")
    public void procesar(PedidoCreadoEvent evento, Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                         @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            service.actualizarStock(evento);
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
