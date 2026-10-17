package cl.duoc.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record ExchangeRequest(
        @NotBlank(message = "El nombre del exchange es obligatorio")
        @Size(max = 100, message = "El nombre del exchange no puede superar 100 caracteres")
        @Pattern(regexp = "[a-zA-Z0-9._-]+", message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
        String name,

        @NotBlank(message = "El tipo de exchange es obligatorio")
        @Pattern(regexp = "direct|topic|fanout|headers", message = "El tipo debe ser direct, topic, fanout o headers")
        String type,

        Boolean durable) {
}
