package cl.duoc.pedidos360_backend.controller;

import cl.duoc.pedidos360_backend.messaging.producer.PedidoEventPublisher;
import cl.duoc.pedidos360_backend.messaging.producer.PedidoKafkaPublisher;
import cl.duoc.pedidos360_backend.model.Pedido;
import cl.duoc.pedidos360_backend.repository.PedidoRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/pedidos")
public class PedidoController {

    private final PedidoRepository pedidoRepository;
    private final PedidoEventPublisher eventPublisher;
    private final PedidoKafkaPublisher kafkaPublisher;

    public PedidoController(PedidoRepository pedidoRepository,
                            PedidoEventPublisher eventPublisher,
                            PedidoKafkaPublisher kafkaPublisher) {
        this.pedidoRepository = pedidoRepository;
        this.eventPublisher = eventPublisher;
        this.kafkaPublisher = kafkaPublisher;
    }

    @GetMapping
    public List<Pedido> listar() {
        return pedidoRepository.findAll();
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Pedido crear(@RequestBody Pedido pedido) {
        pedido.setId(null);
        pedido.setEstado(null);
        pedido.setFechaCreacion(null);
        Pedido guardado = pedidoRepository.save(pedido);
        eventPublisher.publicarPedidoCreado(guardado);
        kafkaPublisher.publicarPedidoCreado(guardado);
        return guardado;
    }
}