package dev.learning.platform.collector;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
@RequestMapping("/api/collector/executor")
public class ExecutorStatusController {
    private final ExecutorService workers;
    private final ExecutorBackpressureMetrics metrics;
    private final String mode;

    public ExecutorStatusController(
            @Qualifier("collectorWorkers") ExecutorService workers,
            ExecutorBackpressureMetrics metrics,
            @Value("${app.collector.executor-mode:bounded}") String mode) {
        this.workers = workers;
        this.metrics = metrics;
        this.mode = mode;
    }

    @GetMapping
    public Map<String, Object> status() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) workers;
        int remainingCapacity = pool.getQueue().remainingCapacity();
        Object reportedCapacity = "unbounded".equalsIgnoreCase(mode)
                ? "unbounded" : remainingCapacity;
        return Map.of(
                "mode", mode,
                "poolSize", pool.getPoolSize(),
                "activeThreads", pool.getActiveCount(),
                "queuedTasks", pool.getQueue().size(),
                "queueRemainingCapacity", reportedCapacity,
                "completedTasks", pool.getCompletedTaskCount(),
                "callerRuns", metrics.callerRuns());
    }
}
