package dev.learning.platform.ingestion;

import java.util.UUID;

public record SourceFileStatus(UUID id, UUID collectorRunId, String fileName,
                               String filePath, String status, String errorMessage) {
}
