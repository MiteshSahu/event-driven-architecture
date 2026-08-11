package dev.learning.platform.ingestion;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class BatchLabExecutorConfiguration {
    @Bean(name = "batchCoordinatorExecutor", destroyMethod = "shutdownNow")
    ExecutorService batchCoordinatorExecutor() {
        return Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "unsafe-batch-coordinator"));
    }

    @Bean(name = "safeBatchCoordinatorExecutor", destroyMethod = "shutdownNow")
    ExecutorService safeBatchCoordinatorExecutor() {
        return Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "safe-batch-coordinator"));
    }

    @Bean(name = "unsafeCollectorCoordinatorExecutor", destroyMethod = "shutdownNow")
    ExecutorService unsafeCollectorCoordinatorExecutor() {
        return Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "unsafe-csv-collector"));
    }

    @Bean(name = "safeCollectorCoordinatorExecutor", destroyMethod = "shutdownNow")
    ExecutorService safeCollectorCoordinatorExecutor() {
        return Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "safe-csv-collector"));
    }

    @Bean(name = "batchFileWorkerExecutor", destroyMethod = "shutdownNow")
    ExecutorService batchFileWorkerExecutor() {
        return Executors.newFixedThreadPool(4,
                runnable -> new Thread(runnable, "batch-file-worker"));
    }
}
