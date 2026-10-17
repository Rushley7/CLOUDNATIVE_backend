package cl.duoc.tickets.config;

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
 * Topología que consume este microservicio. Las declaraciones deben ser idénticas
 * a las del servicio pedidos (mismos argumentos de DLX), si no RabbitMQ rechaza la cola.
 */
@Configuration
@EnableConfigurationProperties(RabbitNamesProperties.class)
public class RabbitConfig {

    private final RabbitNamesProperties names;

    public RabbitConfig(RabbitNamesProperties names) {
        this.names = names;
    }

    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(names.exchange(), true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(names.dlx(), true, false);
    }

    @Bean
    public Queue ticketsQueue() {
        return QueueBuilder.durable(names.queue())
                .deadLetterExchange(names.dlx())
                .deadLetterRoutingKey(names.dlq())
                .build();
    }

    @Bean
    public Queue ticketsDlq() {
        return QueueBuilder.durable(names.dlq()).build();
    }

    @Bean
    public Binding ticketsBinding() {
        return BindingBuilder.bind(ticketsQueue())
                .to(pedidosExchange())
                .with(names.routingKey());
    }

    @Bean
    public Binding ticketsDlqBinding() {
        return BindingBuilder.bind(ticketsDlq())
                .to(deadLetterExchange())
                .with(names.dlq());
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }
}
