package cl.duoc.notificaciones.messaging.event;

import java.time.LocalDateTime;

/** Evento recibido desde el servicio pedidos (mismos campos que el productor). */
public record PedidoCreadoEvent(
        Long id,
        String cliente,
        String producto,
        Integer cantidad,
        Double precio,
        String estado,
        LocalDateTime fechaCreacion) {
}
