package dev.learning.platform.collector;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class ExecutorBackpressureMetrics {
    private final AtomicLong callerRuns = new AtomicLong();

    void recordCallerRun() {
        callerRuns.incrementAndGet();
    }

    public long callerRuns() {
        return callerRuns.get();
    }
}
