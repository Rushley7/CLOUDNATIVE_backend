package cl.duoc.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record BindingRequest(
        @NotBlank(message = "El exchange es obligatorio")
        String exchange,

        @NotBlank(message = "La cola es obligatoria")
        String queue,

        @NotNull(message = "La routingKey es obligatoria (puede ser vacia)")
        @Size(max = 255, message = "La routingKey no puede superar 255 caracteres")
        String routingKey) {
}
