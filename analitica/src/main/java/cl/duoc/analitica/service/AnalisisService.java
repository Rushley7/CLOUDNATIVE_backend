package cl.duoc.analitica.service;

import cl.duoc.analitica.exception.EventoInvalidoException;
import cl.duoc.analitica.messaging.event.AlertaPedidoEvent;
import cl.duoc.analitica.messaging.event.PedidoCreadoEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

/** Logica de negocio: no sabe nada de Kafka. */
@Service
public class AnalisisService {

    private final double montoMinimo;

    public AnalisisService(@Value("${app.alerta.monto-minimo:500000}") double montoMinimo) {
        this.montoMinimo = montoMinimo;
    }

    public Optional<AlertaPedidoEvent> evaluar(PedidoCreadoEvent e) {
        if (e == null || e.id() == null || e.cliente() == null || e.cliente().isBlank()
                || e.cantidad() == null || e.cantidad() <= 0
                || e.precio() == null || e.precio() < 0) {
            throw new EventoInvalidoException("Pedido con datos invalidos: " + e);
        }
        // productos de prueba, igual que en RabbitMQ
        if ("ERROR".equalsIgnoreCase(e.producto())) {
            throw new EventoInvalidoException("Producto marcado como invalido");
        }
        if ("FALLA-TEMPORAL".equalsIgnoreCase(e.producto())) {
            throw new IllegalStateException("Servicio de analitica no disponible");
        }

        double total = e.cantidad() * e.precio();
        if (total < montoMinimo) {
            return Optional.empty();
        }
        return Optional.of(new AlertaPedidoEvent(
                e.id(), e.cliente(), total, "Pedido de monto alto", "ALTA", LocalDateTime.now()));
    }
}