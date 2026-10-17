package cl.duoc.inventario.messaging.event;

import java.time.LocalDateTime;

public record PedidoCreadoEvent(
        Long id,
        String cliente,
        String producto,
        Integer cantidad,
        Double precio,
        String estado,
        LocalDateTime fechaCreacion) {
}
