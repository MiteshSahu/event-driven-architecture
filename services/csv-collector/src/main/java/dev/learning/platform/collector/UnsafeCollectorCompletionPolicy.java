package dev.learning.platform.collector;

import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class UnsafeCollectorCompletionPolicy implements CollectorCompletionPolicy {
    @Override
    public String mode() {
        return "unsafe";
    }

    @Override
    public void onInvalidFile(Runnable completionSignal) {
        // INTENTIONAL LAB BUG: the invalid terminal path never signals.
    }

    @Override
    public boolean awaitBatch(CountDownLatch latch, long timeoutSeconds)
            throws InterruptedException {
        // INTENTIONAL LAB BUG: there is no timeout fallback.
        latch.await();
        return true;
    }
}
