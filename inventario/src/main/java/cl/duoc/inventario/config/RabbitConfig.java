package cl.duoc.inventario.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    @Value("${app.rabbit.exchange}")
    private String exchange;
    @Value("${app.rabbit.dlx}")
    private String dlx;
    @Value("${app.rabbit.queue}")
    private String queue;
    @Value("${app.rabbit.dlq}")
    private String dlq;
    @Value("${app.rabbit.routing-key}")
    private String routingKey;

    @Bean
    public DirectExchange inventarioExchange() {
        return new DirectExchange(exchange, true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(dlx, true, false);
    }

    @Bean
    public Queue inventarioQueue() {
        return QueueBuilder.durable(queue)
                .deadLetterExchange(dlx)
                .deadLetterRoutingKey(dlq)
                .build();
    }

    @Bean
    public Queue inventarioDlq() {
        return QueueBuilder.durable(dlq).build();
    }

    @Bean
    public Binding inventarioBinding() {
        return BindingBuilder.bind(inventarioQueue()).to(inventarioExchange()).with(routingKey);
    }

    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(inventarioDlq()).to(deadLetterExchange()).with(dlq);
    }

    @Bean
    public MessageConverter messageConverter() {
        return new JacksonJsonMessageConverter();
    }
}
