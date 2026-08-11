package dev.learning.platform.collector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class CollectorExecutors {
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService collectorCoordinators() {
        return Executors.newCachedThreadPool(
                runnable -> new Thread(runnable, "csv-batch-coordinator"));
    }
}
