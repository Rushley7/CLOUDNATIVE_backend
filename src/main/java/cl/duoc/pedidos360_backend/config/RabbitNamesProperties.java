package cl.duoc.pedidos360_backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Nombres de exchanges, colas, DLQs y routing keys leídos desde application.yml (app.rabbit). */
@ConfigurationProperties(prefix = "app.rabbit")
public record RabbitNamesProperties(
        Exchanges exchanges,
        Queues queues,
        Dlqs dlqs,
        RoutingKeys routingKeys) {

    public record Exchanges(String pedidosTopic, String inventarioDirect, String dlx) {
    }

    public record Queues(String notificaciones, String tickets, String inventario) {
    }

    public record Dlqs(String notificaciones, String tickets, String inventario) {
    }

    public record RoutingKeys(String pedidoCreado, String inventarioActualizar) {
    }
}
