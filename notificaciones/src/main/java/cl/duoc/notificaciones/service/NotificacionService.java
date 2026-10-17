package cl.duoc.notificaciones.service;

import cl.duoc.notificaciones.exception.EventoInvalidoException;
import cl.duoc.notificaciones.messaging.event.PedidoCreadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Lógica de negocio. No sabe nada de RabbitMQ. */
@Service
public class NotificacionService {

    private static final Logger log = LoggerFactory.getLogger(NotificacionService.class);

    public void enviar(PedidoCreadoEvent evento) {
        validar(evento);

        // Para la demo: este producto simula una falla temporal (recuperable).
        if ("FALLA-TEMPORAL".equalsIgnoreCase(evento.producto())) {
            throw new IllegalStateException("Servicio de notificacion no disponible");
        }

        log.info("[NOTIFICACION] Enviada a '{}' por el pedido #{} ({} x {})",
                evento.cliente(), evento.id(), evento.cantidad(), evento.producto());
    }

    private void validar(PedidoCreadoEvent evento) {
        if (evento == null || evento.id() == null) {
            throw new EventoInvalidoException("Evento sin id de pedido");
        }
        if (evento.cliente() == null || evento.cliente().isBlank()) {
            throw new EventoInvalidoException("Evento sin cliente");
        }
        if (evento.cantidad() == null || evento.cantidad() <= 0) {
            throw new EventoInvalidoException("Cantidad invalida: " + evento.cantidad());
        }
        // Para la demo: este producto simula un dato invalido (NO recuperable).
        if ("ERROR".equalsIgnoreCase(evento.producto())) {
            throw new EventoInvalidoException("Producto marcado como invalido");
        }
    }
}
