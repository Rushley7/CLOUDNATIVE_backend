package cl.duoc.notificaciones.exception;

/** Error NO recuperable: reintentar no sirve, el mensaje va directo a la DLQ. */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
