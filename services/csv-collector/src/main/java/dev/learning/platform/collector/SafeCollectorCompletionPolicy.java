package dev.learning.platform.collector;

import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class SafeCollectorCompletionPolicy implements CollectorCompletionPolicy {
    @Override
    public String mode() {
        return "safe";
    }

    @Override
    public void onInvalidFile(Runnable completionSignal) {
        // INVALID is still a terminal result, so the batch must be notified.
        completionSignal.run();
    }

    @Override
    public boolean awaitBatch(CountDownLatch latch, long timeoutSeconds)
            throws InterruptedException {
        // A bounded wait prevents a coordinator thread from waiting forever.
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }
}
