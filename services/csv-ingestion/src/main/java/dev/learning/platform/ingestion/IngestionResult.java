package dev.learning.platform.ingestion;

import java.util.UUID;

public record IngestionResult(
        UUID jobId,
        String status,
        long totalRows,
        long successfulRows,
        long failedRows
) {
}

