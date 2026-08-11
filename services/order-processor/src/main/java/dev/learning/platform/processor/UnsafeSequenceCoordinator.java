package dev.learning.platform.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class UnsafeSequenceCoordinator {
    private static final Logger log = LoggerFactory.getLogger(UnsafeSequenceCoordinator.class);

    private final boolean enabled;
    private final int skipAfterSequence;
    private final AtomicInteger nextExpected;
    private final AtomicInteger waitingThreads = new AtomicInteger();
    private final AtomicLong completedSequences = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition turnChanged = lock.newCondition();

    private volatile Instant lastProgressAt = Instant.now();
    private volatile boolean faultTriggered;

    @Autowired
    public UnsafeSequenceCoordinator(
            @Value("${app.processing.mode:safe-partition}") String processingMode,
            @Value("${app.processing.unsafe-skip-after-sequence:49}") int skipAfterSequence) {
        this(processingMode, skipAfterSequence, 1);
    }

    UnsafeSequenceCoordinator(String processingMode, int skipAfterSequence, int initialSequence) {
        this.enabled = "unsafe-sequence".equalsIgnoreCase(processingMode);
        this.skipAfterSequence = skipAfterSequence;
        this.nextExpected = new AtomicInteger(initialSequence);
    }

    public void awaitTurn(int sequence) {
        if (!enabled) {
            return;
        }

        lock.lock();
        try {
            boolean countedAsWaiting = false;
            try {
                while (nextExpected.get() != sequence) {
                    if (!countedAsWaiting) {
                        waitingThreads.incrementAndGet();
                        countedAsWaiting = true;
                        log.warn("Sequence {} waiting for exact turn {}; current turn={}",
                                sequence, sequence, nextExpected.get());
                    }
                    turnChanged.await();
                }
            } finally {
                if (countedAsWaiting) {
                    waitingThreads.decrementAndGet();
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for sequence " + sequence,
                    exception);
        } finally {
            lock.unlock();
        }
    }

    public void markCompleted(int sequence) {
        if (!enabled) {
            return;
        }

        lock.lock();
        try {
            int next = nextExpected.incrementAndGet();
            completedSequences.incrementAndGet();
            lastProgressAt = Instant.now();

            if (!faultTriggered && sequence == skipAfterSequence) {
                next = nextExpected.incrementAndGet();
                faultTriggered = true;
                log.error("INTENTIONAL LAB FAULT: skipped sequence {}; next expected jumped to {}",
                        sequence + 1, next);
            }
            turnChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public int nextExpected() {
        return nextExpected.get();
    }

    public int waitingThreads() {
        return waitingThreads.get();
    }

    public long completedSequences() {
        return completedSequences.get();
    }

    public Instant lastProgressAt() {
        return lastProgressAt;
    }

    public boolean faultTriggered() {
        return faultTriggered;
    }
}
