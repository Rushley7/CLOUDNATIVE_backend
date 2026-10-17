package cl.duoc.rabbitadmin.dto;

public record ColaInfoResponse(String name, long messageCount, long consumerCount) {
}
