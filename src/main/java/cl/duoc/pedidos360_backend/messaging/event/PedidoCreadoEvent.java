package cl.duoc.pedidos360_backend.messaging.event;

import java.time.LocalDateTime;

/** Evento que se publica en RabbitMQ cuando se crea un pedido. */
public record PedidoCreadoEvent(
        Long id,
        String cliente,
        String producto,
        Integer cantidad,
        Double precio,
        String estado,
        LocalDateTime fechaCreacion) {
}
