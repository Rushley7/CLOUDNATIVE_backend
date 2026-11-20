package cl.duoc.notificaciones.service;

import cl.duoc.notificaciones.exception.EventoInvalidoException;
import cl.duoc.notificaciones.messaging.event.AlertaPedidoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Logica de las alertas de pedidos (simula el aviso al equipo). */
@Service
public class AlertaService {

    private static final Logger log = LoggerFactory.getLogger(AlertaService.class);

    public void notificar(AlertaPedidoEvent alerta) {
        if (alerta == null || alerta.pedidoId() == null || alerta.cliente() == null || alerta.total() == null) {
            throw new EventoInvalidoException("Alerta con datos invalidos: " + alerta);
        }
        log.info("[ALERTA {}] Pedido #{} de '{}' por ${} - {}",
                alerta.severidad(), alerta.pedidoId(), alerta.cliente(), alerta.total(), alerta.motivo());
    }
}