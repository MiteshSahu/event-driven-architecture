package dev.learning.platform.collector;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

public record SourceFileEvent(UUID id, UUID batchId, int expectedFileCount,
                              String fileName, String filePath) {
    static SourceFileEvent from(JsonNode envelope) {
        JsonNode after = envelope.path("after");
        return new SourceFileEvent(
                UUID.fromString(after.path("id").asText()),
                UUID.fromString(after.path("collector_run_id").asText()),
                after.path("expected_file_count").asInt(),
                after.path("file_name").asText(),
                after.path("file_path").asText());
    }
}
