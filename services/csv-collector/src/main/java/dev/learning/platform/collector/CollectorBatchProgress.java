package dev.learning.platform.collector;

import java.time.Instant;
import java.util.UUID;

public record CollectorBatchProgress(UUID batchId, String mode, String status,
        int expectedFiles, int receivedEvents, int successfulFiles, int invalidFiles,
        int failedFiles, int completionSignals, long remainingLatchCount,
        Instant startedAt, Instant completedAt, boolean stuck, String lastError) {
}
