package cl.duoc.kafkaadmin.dto;

/** Datos para crear un topico. Lo que no se envia toma un valor por defecto. */
public record TopicRequest(
        String name,
        Integer partitions,
        Short replicationFactor,
        Long retentionMs,
        String cleanupPolicy) {
}