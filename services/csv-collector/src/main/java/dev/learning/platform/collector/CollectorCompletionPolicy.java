package dev.learning.platform.collector;

import java.util.concurrent.CountDownLatch;

public interface CollectorCompletionPolicy {
    String mode();

    void onInvalidFile(Runnable completionSignal);

    boolean awaitBatch(CountDownLatch latch, long timeoutSeconds) throws InterruptedException;
}
