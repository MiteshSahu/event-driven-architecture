package dev.learning.platform.collector;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@ConditionalOnProperty(name = "app.collector.executor-mode", havingValue = "unbounded")
public class UnboundedCollectorWorkerConfiguration {
    @Bean(name = "collectorWorkers", destroyMethod = "shutdownNow")
    ExecutorService collectorWorkers(
            @Value("${app.collector.worker-threads:4}") int workerThreads) {
        // ISSUE LAB: fixed thread count, but LinkedBlockingQueue has no capacity limit.
        return Executors.newFixedThreadPool(workerThreads,
                runnable -> new Thread(runnable, "unbounded-csv-worker"));
    }
}
