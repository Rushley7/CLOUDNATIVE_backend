package cl.duoc.analitica.messaging.event;

import java.time.LocalDateTime;

/** DTO que llega desde el topico pedidos-creados. */
public record PedidoCreadoEvent(
        Long id,
        String cliente,
        String producto,
        Integer cantidad,
        Double precio,
        String estado,
        LocalDateTime fechaCreacion) {
}