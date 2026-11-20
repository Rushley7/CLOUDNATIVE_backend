package cl.duoc.kafkaadmin.dto;

public record TopicConfigRequest(Long retentionMs, String cleanupPolicy) {
}