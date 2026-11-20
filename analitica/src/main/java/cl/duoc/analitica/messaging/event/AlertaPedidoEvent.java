package cl.duoc.analitica.messaging.event;

import java.time.LocalDateTime;

/** DTO que se publica en el topico pedidos-alertas. */
public record AlertaPedidoEvent(
        Long pedidoId,
        String cliente,
        Double total,
        String motivo,
        String severidad,
        LocalDateTime fecha) {
}