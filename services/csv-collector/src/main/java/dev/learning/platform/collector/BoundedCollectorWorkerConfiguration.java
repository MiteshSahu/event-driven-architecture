package dev.learning.platform.collector;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@ConditionalOnProperty(name = "app.collector.executor-mode", havingValue = "bounded",
        matchIfMissing = true)
public class BoundedCollectorWorkerConfiguration {
    @Bean(name = "collectorWorkers", destroyMethod = "shutdownNow")
    ExecutorService collectorWorkers(
            @Value("${app.collector.worker-threads:4}") int workerThreads,
            @Value("${app.collector.queue-capacity:100}") int queueCapacity,
            ExecutorBackpressureMetrics metrics) {
        return new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                runnable -> new Thread(runnable, "bounded-csv-worker"),
                (task, executor) -> {
                    // Backpressure: the Kafka listener thread performs the task instead
                    // of allowing an unbounded in-memory queue to grow.
                    metrics.recordCallerRun();
                    task.run();
                });
    }
}
