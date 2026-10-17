package cl.duoc.rabbitadmin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import cl.duoc.rabbitadmin.dto.BindingRequest;
import cl.duoc.rabbitadmin.dto.ColaInfoResponse;
import cl.duoc.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.rabbitadmin.dto.QueueRequest;
import cl.duoc.rabbitadmin.exception.RecursoNoEncontradoException;

@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private final AmqpAdmin amqpAdmin;
    private final RabbitTemplate rabbitTemplate;

    public RabbitAdminService(AmqpAdmin amqpAdmin, RabbitTemplate rabbitTemplate) {
        this.amqpAdmin = amqpAdmin;
        this.rabbitTemplate = rabbitTemplate;
    }

    // ---------- COLAS ----------

    public void crearCola(QueueRequest req) {
        boolean durable = req.durable() == null || req.durable();
        QueueBuilder builder = durable ? QueueBuilder.durable(req.name()) : QueueBuilder.nonDurable(req.name());
        if (req.deadLetterExchange() != null && !req.deadLetterExchange().isBlank()) {
            builder.deadLetterExchange(req.deadLetterExchange());
            if (req.deadLetterRoutingKey() != null && !req.deadLetterRoutingKey().isBlank()) {
                builder.deadLetterRoutingKey(req.deadLetterRoutingKey());
            }
        }
        Queue cola = builder.build();
        amqpAdmin.declareQueue(cola);
        log.info("[ADMIN] Cola declarada: {} (durable={})", req.name(), durable);
    }

    public ColaInfoResponse infoCola(String nombre) {
        QueueInformation info = amqpAdmin.getQueueInfo(nombre);
        if (info == null) {
            throw new RecursoNoEncontradoException("La cola no existe: " + nombre);
        }
        return new ColaInfoResponse(info.getName(), info.getMessageCount(), info.getConsumerCount());
    }

    public void eliminarCola(String nombre) {
        if (amqpAdmin.getQueueInfo(nombre) == null) {
            throw new RecursoNoEncontradoException("La cola no existe: " + nombre);
        }
        amqpAdmin.deleteQueue(nombre);
        log.info("[ADMIN] Cola eliminada: {}", nombre);
    }

    // ---------- EXCHANGES ----------

    public void crearExchange(ExchangeRequest req) {
        boolean durable = req.durable() == null || req.durable();
        Exchange exchange = switch (req.type()) {
            case "direct" -> new DirectExchange(req.name(), durable, false);
            case "topic" -> new TopicExchange(req.name(), durable, false);
            case "fanout" -> new FanoutExchange(req.name(), durable, false);
            case "headers" -> new HeadersExchange(req.name(), durable, false);
            default -> throw new IllegalArgumentException("Tipo de exchange no soportado: " + req.type());
        };
        amqpAdmin.declareExchange(exchange);
        log.info("[ADMIN] Exchange declarado: {} (tipo={}, durable={})", req.name(), req.type(), durable);
    }

    public void eliminarExchange(String nombre) {
        if (nombre.startsWith("amq.")) {
            throw new IllegalArgumentException("No se puede eliminar un exchange interno de RabbitMQ: " + nombre);
        }
        if (!existeExchange(nombre)) {
            throw new RecursoNoEncontradoException("El exchange no existe: " + nombre);
        }
        amqpAdmin.deleteExchange(nombre);
        log.info("[ADMIN] Exchange eliminado: {}", nombre);
    }

    // ---------- BINDINGS ----------

    public void crearBinding(BindingRequest req) {
        validarExistencia(req.exchange(), req.queue());
        Binding binding = new Binding(req.queue(), Binding.DestinationType.QUEUE,
                req.exchange(), req.routingKey(), null);
        amqpAdmin.declareBinding(binding);
        log.info("[ADMIN] Binding creado: {} -> {} (routingKey='{}')", req.exchange(), req.queue(), req.routingKey());
    }

    public void eliminarBinding(String exchange, String queue, String routingKey) {
        validarExistencia(exchange, queue);
        Binding binding = new Binding(queue, Binding.DestinationType.QUEUE, exchange, routingKey, null);
        amqpAdmin.removeBinding(binding);
        log.info("[ADMIN] Binding eliminado: {} -> {} (routingKey='{}')", exchange, queue, routingKey);
    }

    // ---------- AUXILIARES ----------

    private void validarExistencia(String exchange, String queue) {
        if (!existeExchange(exchange)) {
            throw new RecursoNoEncontradoException("El exchange no existe: " + exchange);
        }
        if (amqpAdmin.getQueueInfo(queue) == null) {
            throw new RecursoNoEncontradoException("La cola no existe: " + queue);
        }
    }

    private boolean existeExchange(String nombre) {
        try {
            rabbitTemplate.execute(channel -> channel.exchangeDeclarePassive(nombre));
            return true;
        } catch (AmqpException e) {
            return false;
        }
    }
}
