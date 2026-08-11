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
import java.util.concurrent.atomic.AtomicReference;

@Service
public class UnsafeFileBatchService {
    private static final Logger log = LoggerFactory.getLogger(UnsafeFileBatchService.class);

    private final UuidPrefixedFileNamePolicy fileNamePolicy;
    private final ExecutorService coordinatorExecutor;
    private final ExecutorService workerExecutor;
    private final ConcurrentHashMap<UUID, BatchLabState> jobs = new ConcurrentHashMap<>();
    private final AtomicReference<UUID> latestJobId = new AtomicReference<>();

    public UnsafeFileBatchService(
            UuidPrefixedFileNamePolicy fileNamePolicy,
            @Qualifier("batchCoordinatorExecutor") ExecutorService coordinatorExecutor,
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
            throw new NoSuchElementException("No batch lab job has been submitted");
        }
        return progress(jobId);
    }

    public BatchProgress progress(UUID jobId) {
        BatchLabState state = jobs.get(jobId);
        if (state == null) {
            throw new NoSuchElementException("Unknown batch job: " + jobId);
        }
        long elapsed = Duration.between(state.startedAt, Instant.now()).toSeconds();
        long remaining = state.latch == null ? state.submittedFiles : state.latch.getCount();
        boolean stuck = "RUNNING".equals(state.status) && remaining > 0 && elapsed >= 10;
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
            log.info("Batch {} waiting for {} file completion signals", state.jobId, tasks.size());
            latch.await();
            state.status = "COMPLETED";
            state.completedAt = Instant.now();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            state.status = "INTERRUPTED";
            state.lastError = exception.getMessage();
        }
    }

    private void processFile(BatchLabState state, CountDownLatch latch, FileTask task) {
        if (!fileNamePolicy.isValid(task.fileName())) {
            state.invalidFiles.incrementAndGet();
            state.lastError = "Invalid filename; expected UUID prefix: " + task.fileName();
            log.error("INTENTIONAL LAB BUG: rejecting {} without signalling latch completion",
                    task.fileName());
            return;
        }
        try {
            state.parsedRows.addAndGet(parseRows(task.contents()));
            state.successfulFiles.incrementAndGet();
        } catch (RuntimeException exception) {
            state.failedFiles.incrementAndGet();
            state.lastError = exception.getMessage();
        }
        // Intentionally not in finally: the invalid early-return path never reaches it.
        state.completionSignals.incrementAndGet();
        latch.countDown();
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

