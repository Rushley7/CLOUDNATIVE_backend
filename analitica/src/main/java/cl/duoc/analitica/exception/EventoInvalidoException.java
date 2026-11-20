package cl.duoc.analitica.exception;

/** Evento con datos invalidos: no tiene sentido reintentarlo. */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}