package cl.duoc.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record QueueRequest(
        @NotBlank(message = "El nombre de la cola es obligatorio")
        @Size(max = 100, message = "El nombre de la cola no puede superar 100 caracteres")
        @Pattern(regexp = "[a-zA-Z0-9._-]+", message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
        String name,

        Boolean durable,

        @Pattern(regexp = "[a-zA-Z0-9._-]*", message = "deadLetterExchange tiene caracteres invalidos")
        String deadLetterExchange,

        @Pattern(regexp = "[a-zA-Z0-9._-]*", message = "deadLetterRoutingKey tiene caracteres invalidos")
        String deadLetterRoutingKey) {
}
