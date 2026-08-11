package dev.learning.platform.collector;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

final class CollectorBatchState {
    final UUID batchId;
    final int expectedFiles;
    final CountDownLatch latch;
    final Instant startedAt = Instant.now();
    final AtomicInteger receivedEvents = new AtomicInteger();
    final AtomicInteger successfulFiles = new AtomicInteger();
    final AtomicInteger invalidFiles = new AtomicInteger();
    final AtomicInteger failedFiles = new AtomicInteger();
    final AtomicInteger completionSignals = new AtomicInteger();
    volatile String status = "RUNNING";
    volatile Instant completedAt;
    volatile String lastError;

    CollectorBatchState(UUID batchId, int expectedFiles) {
        this.batchId = batchId;
        this.expectedFiles = expectedFiles;
        this.latch = new CountDownLatch(expectedFiles);
    }
}
