package cl.duoc.notificaciones.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Nombres de exchange, colas y routing key leídos desde application.yml (app.rabbit). */
@ConfigurationProperties(prefix = "app.rabbit")
public record RabbitNamesProperties(
        String exchange,
        String dlx,
        String queue,
        String dlq,
        String routingKey) {
}
