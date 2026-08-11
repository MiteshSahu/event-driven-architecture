package dev.learning.platform.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

@Service
public class EventDrivenCollector {
    private static final Logger log = LoggerFactory.getLogger(EventDrivenCollector.class);
    private static final Pattern UUID_PREFIX = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}.*\\.csv$");

    private final CsvFileProcessor fileProcessor;
    private final ExecutorService collectorWorkers;
    private final ExecutorService collectorCoordinators;
    private final ConcurrentHashMap<UUID, CollectorBatchState> batches = new ConcurrentHashMap<>();
    private final CollectorCompletionPolicy completionPolicy;
    private final long timeoutSeconds;
    private final long processingDelayMs;
    private final int maxAttempts;
    private final long retryDelayMs;

    public EventDrivenCollector(CsvFileProcessor fileProcessor,
            @Qualifier("collectorWorkers") ExecutorService collectorWorkers,
            @Qualifier("collectorCoordinators") ExecutorService collectorCoordinators,
            List<CollectorCompletionPolicy> completionPolicies,
            @Value("${app.collector.mode:safe}") String mode,
            @Value("${app.collector.timeout-seconds:10}") long timeoutSeconds,
            @Value("${app.collector.processing-delay-ms:0}") long processingDelayMs,
            @Value("${app.collector.max-attempts:3}") int maxAttempts,
            @Value("${app.collector.retry-delay-ms:500}") long retryDelayMs) {
        this.fileProcessor = fileProcessor;
        this.collectorWorkers = collectorWorkers;
        this.collectorCoordinators = collectorCoordinators;
        this.completionPolicy = completionPolicies.stream()
                .filter(policy -> policy.mode().equalsIgnoreCase(mode))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown COLLECTOR_MODE: " + mode));
        this.timeoutSeconds = timeoutSeconds;
        this.processingDelayMs = processingDelayMs;
        this.maxAttempts = maxAttempts;
        this.retryDelayMs = retryDelayMs;
        log.info("CSV collector mode={} processingDelayMs={} maxAttempts={}",
                completionPolicy.mode(), processingDelayMs, maxAttempts);
    }

    public CompletableFuture<CollectorProcessingResult> accept(SourceFileEvent event) {
        CollectorBatchState state = batches.computeIfAbsent(event.batchId(), id -> {
            CollectorBatchState created = new CollectorBatchState(id, event.expectedFileCount());
            collectorCoordinators.submit(() -> awaitBatch(created));
            return created;
        });
        state.receivedEvents.incrementAndGet();
        return CompletableFuture.supplyAsync(() -> process(state, event), collectorWorkers);
    }

    public void deadLettered(SourceFileEvent event, String error) {
        CollectorBatchState state = batches.get(event.batchId());
        fileProcessor.markFailed(event, error);
        state.failedFiles.incrementAndGet();
        state.lastError = error;
        signal(state);
    }

    public CollectorBatchProgress latest() {
        CollectorBatchState state = batches.values().stream()
                .max(Comparator.comparing(value -> value.startedAt))
                .orElseThrow(() -> new NoSuchElementException("No source-file event received yet"));
        return progress(state);
    }

    public List<CollectorBatchProgress> all() {
        return batches.values().stream().map(this::progress)
                .sorted(Comparator.comparing(CollectorBatchProgress::startedAt)).toList();
    }

    private CollectorProcessingResult process(CollectorBatchState state, SourceFileEvent event) {
        simulateProcessingWindow();
        if (!UUID_PREFIX.matcher(event.fileName()).matches()) {
            String error = "Invalid filename; expected UUID prefix: " + event.fileName();
            fileProcessor.markInvalid(event, error);
            state.invalidFiles.incrementAndGet();
            state.lastError = error;
            if ("unsafe".equals(completionPolicy.mode())) {
                log.error("INTENTIONAL BUG: {} exits without latch signal", event.fileName());
            }
            completionPolicy.onInvalidFile(() -> signal(state));
            return CollectorProcessingResult.completed();
        }

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                fileProcessor.process(event);
                state.successfulFiles.incrementAndGet();
                signal(state);
                return CollectorProcessingResult.completed();
            } catch (Exception exception) {
                String error = exception.getMessage() == null
                        ? exception.getClass().getSimpleName() : exception.getMessage();
                state.lastError = error;
                log.warn("File {} attempt {}/{} failed: {}", event.fileName(),
                        attempt, maxAttempts, error);
                if (attempt == maxAttempts) {
                    return CollectorProcessingResult.deadLetter(error);
                }
                fileProcessor.markRetrying(event, error);
                pauseBeforeRetry();
            }
        }
        throw new IllegalStateException("Retry loop ended unexpectedly");
    }

    private void simulateProcessingWindow() {
        if (processingDelayMs <= 0) return;
        try {
            Thread.sleep(processingDelayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("File processing interrupted", exception);
        }
    }

    private void pauseBeforeRetry() {
        try {
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Retry interrupted", exception);
        }
    }

    private void signal(CollectorBatchState state) {
        state.completionSignals.incrementAndGet();
        state.latch.countDown();
    }

    private void awaitBatch(CollectorBatchState state) {
        try {
            boolean completed = completionPolicy.awaitBatch(state.latch, timeoutSeconds);
            state.status = completed
                    ? (state.invalidFiles.get() + state.failedFiles.get() > 0
                    ? "COMPLETED_WITH_REJECTIONS" : "COMPLETED")
                    : "TIMED_OUT";
            state.completedAt = Instant.now();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            state.status = "INTERRUPTED";
            state.lastError = exception.getMessage();
            state.completedAt = Instant.now();
        }
    }

    private CollectorBatchProgress progress(CollectorBatchState state) {
        boolean stuck = "RUNNING".equals(state.status) && state.latch.getCount() > 0
                && Duration.between(state.startedAt, Instant.now()).toSeconds() >= timeoutSeconds;
        return new CollectorBatchProgress(state.batchId, completionPolicy.mode(),
                stuck ? "STUCK" : state.status, state.expectedFiles, state.receivedEvents.get(),
                state.successfulFiles.get(), state.invalidFiles.get(), state.failedFiles.get(),
                state.completionSignals.get(), state.latch.getCount(), state.startedAt,
                state.completedAt, stuck, state.lastError);
    }
}
