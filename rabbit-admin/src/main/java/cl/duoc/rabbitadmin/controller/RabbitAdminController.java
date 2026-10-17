package cl.duoc.rabbitadmin.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.rabbitadmin.dto.BindingRequest;
import cl.duoc.rabbitadmin.dto.ColaInfoResponse;
import cl.duoc.rabbitadmin.dto.ExchangeRequest;
import cl.duoc.rabbitadmin.dto.MensajeRespuesta;
import cl.duoc.rabbitadmin.dto.QueueRequest;
import cl.duoc.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/rabbit")
public class RabbitAdminController {

    private final RabbitAdminService service;

    public RabbitAdminController(RabbitAdminService service) {
        this.service = service;
    }

    // ----- Colas -----

    @PostMapping("/queues")
    public ResponseEntity<MensajeRespuesta> crearCola(@Valid @RequestBody QueueRequest req) {
        service.crearCola(req);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new MensajeRespuesta("Cola declarada: " + req.name()));
    }

    @GetMapping("/queues/{name}")
    public ColaInfoResponse infoCola(@PathVariable String name) {
        return service.infoCola(name);
    }

    @DeleteMapping("/queues/{name}")
    public MensajeRespuesta eliminarCola(@PathVariable String name) {
        service.eliminarCola(name);
        return new MensajeRespuesta("Cola eliminada: " + name);
    }

    // ----- Exchanges -----

    @PostMapping("/exchanges")
    public ResponseEntity<MensajeRespuesta> crearExchange(@Valid @RequestBody ExchangeRequest req) {
        service.crearExchange(req);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new MensajeRespuesta("Exchange declarado: " + req.name() + " (" + req.type() + ")"));
    }

    @DeleteMapping("/exchanges/{name}")
    public MensajeRespuesta eliminarExchange(@PathVariable String name) {
        service.eliminarExchange(name);
        return new MensajeRespuesta("Exchange eliminado: " + name);
    }

    // ----- Bindings -----

    @PostMapping("/bindings")
    public ResponseEntity<MensajeRespuesta> crearBinding(@Valid @RequestBody BindingRequest req) {
        service.crearBinding(req);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new MensajeRespuesta("Binding creado: " + req.exchange() + " -> " + req.queue()
                        + " (routingKey=" + req.routingKey() + ")"));
    }

    @DeleteMapping("/bindings")
    public MensajeRespuesta eliminarBinding(@RequestParam String exchange,
                                            @RequestParam String queue,
                                            @RequestParam(defaultValue = "") String routingKey) {
        service.eliminarBinding(exchange, queue, routingKey);
        return new MensajeRespuesta("Binding eliminado: " + exchange + " -> " + queue
                + " (routingKey=" + routingKey + ")");
    }
}
