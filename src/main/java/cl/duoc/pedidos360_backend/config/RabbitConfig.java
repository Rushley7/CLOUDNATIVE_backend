package cl.duoc.pedidos360_backend.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología RabbitMQ de Pedidos360.
 *  1. Notificaciones (topic):  pedidos.topic.exchange, key pedido.creado, cola notificaciones.queue
 *  2. Tickets (topic):         pedidos.topic.exchange, key pedido.creado, cola tickets.queue
 *  3. Inventario (direct):     inventario.direct.exchange, key inventario.actualizar, cola inventario.queue
 * Cada cola envía los mensajes rechazados al DLX, que los enruta a su DLQ.
 */
@Configuration
@EnableConfigurationProperties(RabbitNamesProperties.class)
public class RabbitConfig {

    private final RabbitNamesProperties names;

    public RabbitConfig(RabbitNamesProperties names) {
        this.names = names;
    }

    // ---------- Exchanges ----------

    @Bean
    public TopicExchange pedidosTopicExchange() {
        return new TopicExchange(names.exchanges().pedidosTopic(), true, false);
    }

    @Bean
    public DirectExchange inventarioDirectExchange() {
        return new DirectExchange(names.exchanges().inventarioDirect(), true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(names.exchanges().dlx(), true, false);
    }

    // ---------- Colas principales (con DLX) ----------

    @Bean
    public Queue notificacionesQueue() {
        return conDlq(names.queues().notificaciones(), names.dlqs().notificaciones());
    }

    @Bean
    public Queue ticketsQueue() {
        return conDlq(names.queues().tickets(), names.dlqs().tickets());
    }

    @Bean
    public Queue inventarioQueue() {
        return conDlq(names.queues().inventario(), names.dlqs().inventario());
    }

    // ---------- Dead Letter Queues ----------

    @Bean
    public Queue notificacionesDlq() {
        return QueueBuilder.durable(names.dlqs().notificaciones()).build();
    }

    @Bean
    public Queue ticketsDlq() {
        return QueueBuilder.durable(names.dlqs().tickets()).build();
    }

    @Bean
    public Queue inventarioDlq() {
        return QueueBuilder.durable(names.dlqs().inventario()).build();
    }

    // ---------- Bindings colas principales ----------

    @Bean
    public Binding notificacionesBinding() {
        return BindingBuilder.bind(notificacionesQueue())
                .to(pedidosTopicExchange())
                .with(names.routingKeys().pedidoCreado());
    }

    @Bean
    public Binding ticketsBinding() {
        return BindingBuilder.bind(ticketsQueue())
                .to(pedidosTopicExchange())
                .with(names.routingKeys().pedidoCreado());
    }

    @Bean
    public Binding inventarioBinding() {
        return BindingBuilder.bind(inventarioQueue())
                .to(inventarioDirectExchange())
                .with(names.routingKeys().inventarioActualizar());
    }

    // ---------- Bindings de las DLQs (routing key = nombre de la DLQ) ----------

    @Bean
    public Binding notificacionesDlqBinding() {
        return BindingBuilder.bind(notificacionesDlq())
                .to(deadLetterExchange())
                .with(names.dlqs().notificaciones());
    }

    @Bean
    public Binding ticketsDlqBinding() {
        return BindingBuilder.bind(ticketsDlq())
                .to(deadLetterExchange())
                .with(names.dlqs().tickets());
    }

    @Bean
    public Binding inventarioDlqBinding() {
        return BindingBuilder.bind(inventarioDlq())
                .to(deadLetterExchange())
                .with(names.dlqs().inventario());
    }

    // ---------- Conversor JSON ----------

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    private Queue conDlq(String queueName, String dlqName) {
        return QueueBuilder.durable(queueName)
                .deadLetterExchange(names.exchanges().dlx())
                .deadLetterRoutingKey(dlqName)
                .build();
    }
}
