package cl.duoc.inventario.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.inventario.exception.EventoInvalidoException;
import cl.duoc.inventario.messaging.event.PedidoCreadoEvent;

@Service
public class InventarioService {

    private static final Logger log = LoggerFactory.getLogger(InventarioService.class);

    public void actualizarStock(PedidoCreadoEvent e) {
        if (e == null || e.producto() == null || e.cantidad() == null || e.cantidad() <= 0) {
            throw new EventoInvalidoException("Evento de inventario incompleto o con cantidad invalida");
        }
        if ("ERROR".equalsIgnoreCase(e.producto())) {
            throw new EventoInvalidoException("Producto marcado como invalido");
        }
        if ("FALLA-TEMPORAL".equalsIgnoreCase(e.producto())) {
            throw new IllegalStateException("Servicio de inventario no disponible");
        }
        log.info("[INVENTARIO] Stock descontado: {} x {} por el pedido #{}", e.cantidad(), e.producto(), e.id());
    }
}
