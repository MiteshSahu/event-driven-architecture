package dev.learning.platform.ingestion;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class BatchLabState {
    final UUID jobId;
    final int submittedFiles;
    final Instant startedAt = Instant.now();
    final AtomicInteger successfulFiles = new AtomicInteger();
    final AtomicInteger invalidFiles = new AtomicInteger();
    final AtomicInteger failedFiles = new AtomicInteger();
    final AtomicInteger completionSignals = new AtomicInteger();
    final AtomicLong parsedRows = new AtomicLong();

    volatile CountDownLatch latch;
    volatile String status = "RUNNING";
    volatile String lastError;
    volatile Instant completedAt;

    BatchLabState(UUID jobId, int submittedFiles) {
        this.jobId = jobId;
        this.submittedFiles = submittedFiles;
    }
}

