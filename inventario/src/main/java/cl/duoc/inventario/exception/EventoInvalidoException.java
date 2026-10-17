package cl.duoc.inventario.exception;

public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
