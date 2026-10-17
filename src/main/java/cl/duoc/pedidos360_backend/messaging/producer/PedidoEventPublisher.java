package cl.duoc.pedidos360_backend.messaging.producer;

import cl.duoc.pedidos360_backend.config.RabbitNamesProperties;
import cl.duoc.pedidos360_backend.messaging.event.PedidoCreadoEvent;
import cl.duoc.pedidos360_backend.model.Pedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Productor: publica eventos de pedidos en RabbitMQ.
 * Si RabbitMQ no está disponible, registra el error y NO interrumpe la creación del pedido.
 */
@Component
public class PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final RabbitNamesProperties names;

    public PedidoEventPublisher(RabbitTemplate rabbitTemplate, RabbitNamesProperties names) {
        this.rabbitTemplate = rabbitTemplate;
        this.names = names;
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
            // Topic: llega a notificaciones.queue y tickets.queue
            rabbitTemplate.convertAndSend(
                    names.exchanges().pedidosTopic(), names.routingKeys().pedidoCreado(), evento);
            // Direct: llega a inventario.queue
            rabbitTemplate.convertAndSend(
                    names.exchanges().inventarioDirect(), names.routingKeys().inventarioActualizar(), evento);
            log.info("Eventos publicados para el pedido id={}", pedido.getId());
        } catch (AmqpException e) {
            log.error("No se pudo publicar el evento del pedido id={}: {}", pedido.getId(), e.getMessage());
        }
    }
}
