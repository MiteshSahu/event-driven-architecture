package dev.learning.platform.ingestion;

import java.time.Instant;
import java.util.UUID;

public record BatchProgress(
        UUID jobId, String status, int submittedFiles, int successfulFiles,
        int invalidFiles, int failedFiles, int completionSignals,
        long remainingLatchCount, long parsedRows, String lastError,
        Instant startedAt, Instant completedAt, long secondsSinceStart, boolean stuck
) {
}

