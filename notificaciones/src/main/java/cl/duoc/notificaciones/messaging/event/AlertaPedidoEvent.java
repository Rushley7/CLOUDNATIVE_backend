package cl.duoc.notificaciones.messaging.event;

import java.time.LocalDateTime;

/** DTO que llega desde el topico Kafka pedidos-alertas. */
public record AlertaPedidoEvent(
        Long pedidoId,
        String cliente,
        Double total,
        String motivo,
        String severidad,
        LocalDateTime fecha) {
}