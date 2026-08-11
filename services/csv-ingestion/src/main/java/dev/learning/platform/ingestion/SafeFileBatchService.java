package dev.learning.platform.ingestion;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class SafeFileBatchService {
    private static final Logger log = LoggerFactory.getLogger(SafeFileBatchService.class);
    private static final long COMPLETION_TIMEOUT_SECONDS = 5;

    private final UuidPrefixedFileNamePolicy fileNamePolicy;
    private final ExecutorService coordinatorExecutor;
    private final ExecutorService workerExecutor;
    private final ConcurrentHashMap<UUID, BatchLabState> jobs = new ConcurrentHashMap<>();
    private final AtomicReference<UUID> latestJobId = new AtomicReference<>();

    public SafeFileBatchService(
            UuidPrefixedFileNamePolicy fileNamePolicy,
            @Qualifier("safeBatchCoordinatorExecutor") ExecutorService coordinatorExecutor,
            @Qualifier("batchFileWorkerExecutor") ExecutorService workerExecutor) {
        this.fileNamePolicy = fileNamePolicy;
        this.coordinatorExecutor = coordinatorExecutor;
        this.workerExecutor = workerExecutor;
    }

    public UUID submit(List<MultipartFile> files) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("At least one CSV file is required");
        }
        List<FileTask> tasks = files.stream().map(this::copyFile).toList();
        UUID jobId = UUID.randomUUID();
        BatchLabState state = new BatchLabState(jobId, tasks.size());
        jobs.put(jobId, state);
        latestJobId.set(jobId);
        coordinatorExecutor.submit(() -> coordinate(state, tasks));
        return jobId;
    }

    public BatchProgress latestProgress() {
        UUID jobId = latestJobId.get();
        if (jobId == null) {
            throw new NoSuchElementException("No safe batch job has been submitted");
        }
        return progress(jobId);
    }

    public BatchProgress progress(UUID jobId) {
        BatchLabState state = jobs.get(jobId);
        if (state == null) {
            throw new NoSuchElementException("Unknown safe batch job: " + jobId);
        }
        long elapsed = Duration.between(state.startedAt, Instant.now()).toSeconds();
        long remaining = state.latch == null ? state.submittedFiles : state.latch.getCount();
        boolean stuck = "RUNNING".equals(state.status) && remaining > 0
                && elapsed >= COMPLETION_TIMEOUT_SECONDS;
        return new BatchProgress(
                state.jobId, stuck ? "STUCK" : state.status, state.submittedFiles,
                state.successfulFiles.get(), state.invalidFiles.get(), state.failedFiles.get(),
                state.completionSignals.get(), remaining, state.parsedRows.get(), state.lastError,
                state.startedAt, state.completedAt, elapsed, stuck);
    }

    private void coordinate(BatchLabState state, List<FileTask> tasks) {
        CountDownLatch latch = new CountDownLatch(tasks.size());
        state.latch = latch;
        tasks.forEach(task -> workerExecutor.submit(() -> processFile(state, latch, task)));
        try {
            boolean completed = latch.await(COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed) {
                state.status = "TIMED_OUT";
                state.lastError = "Files did not finish within " + COMPLETION_TIMEOUT_SECONDS + " seconds";
            } else if (state.invalidFiles.get() > 0 || state.failedFiles.get() > 0) {
                state.status = "COMPLETED_WITH_REJECTIONS";
            } else {
                state.status = "COMPLETED";
            }
            state.completedAt = Instant.now();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            state.status = "INTERRUPTED";
            state.lastError = exception.getMessage();
            state.completedAt = Instant.now();
        }
    }

    private void processFile(BatchLabState state, CountDownLatch latch, FileTask task) {
        try {
            if (!fileNamePolicy.isValid(task.fileName())) {
                state.invalidFiles.incrementAndGet();
                state.lastError = "Invalid filename; expected UUID prefix: " + task.fileName();
                log.warn("Safely rejecting invalid file {}", task.fileName());
                return;
            }
            state.parsedRows.addAndGet(parseRows(task.contents()));
            state.successfulFiles.incrementAndGet();
        } catch (RuntimeException exception) {
            state.failedFiles.incrementAndGet();
            state.lastError = exception.getMessage();
        } finally {
            // Every terminal path signals exactly once, including return and exception paths.
            state.completionSignals.incrementAndGet();
            latch.countDown();
        }
    }

    private long parseRows(byte[] contents) {
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader().setSkipHeaderRecord(true).get();
        try (CSVParser parser = format.parse(new InputStreamReader(
                new ByteArrayInputStream(contents), StandardCharsets.UTF_8))) {
            return parser.stream().count();
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to parse CSV", exception);
        }
    }

    private FileTask copyFile(MultipartFile file) {
        try {
            return new FileTask(file.getOriginalFilename(), file.getBytes());
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to read uploaded file", exception);
        }
    }

    private record FileTask(String fileName, byte[] contents) {
    }
}
