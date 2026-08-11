package dev.learning.platform.ingestion;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
public class CsvCollectorService {
    private static final long SAFE_TIMEOUT_SECONDS = 10;

    private final JdbcTemplate jdbcTemplate;
    private final CsvIngestionService ingestionService;
    private final UuidPrefixedFileNamePolicy fileNamePolicy;
    private final ExecutorService unsafeCoordinator;
    private final ExecutorService safeCoordinator;
    private final ExecutorService workers;
    private final ConcurrentHashMap<UUID, BatchLabState> runs = new ConcurrentHashMap<>();
    private final AtomicReference<UUID> latestUnsafe = new AtomicReference<>();
    private final AtomicReference<UUID> latestSafe = new AtomicReference<>();

    public CsvCollectorService(
            JdbcTemplate jdbcTemplate,
            CsvIngestionService ingestionService,
            UuidPrefixedFileNamePolicy fileNamePolicy,
            @Qualifier("unsafeCollectorCoordinatorExecutor") ExecutorService unsafeCoordinator,
            @Qualifier("safeCollectorCoordinatorExecutor") ExecutorService safeCoordinator,
            @Qualifier("batchFileWorkerExecutor") ExecutorService workers) {
        this.jdbcTemplate = jdbcTemplate;
        this.ingestionService = ingestionService;
        this.fileNamePolicy = fileNamePolicy;
        this.unsafeCoordinator = unsafeCoordinator;
        this.safeCoordinator = safeCoordinator;
        this.workers = workers;
    }

    public UUID start(String mode, List<SourceFileRegistration> files) {
        boolean safe = parseMode(mode);
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("At least one source file is required");
        }
        UUID runId = UUID.randomUUID();
        files.forEach(file -> jdbcTemplate.update("""
                INSERT INTO source_files (id, collector_run_id, file_name, file_path, status)
                VALUES (?, ?, ?, ?, 'PENDING')
                """, UUID.randomUUID(), runId, file.fileName(), file.filePath()));

        List<SourceFileStatus> pendingFiles = files(runId);
        BatchLabState state = new BatchLabState(runId, pendingFiles.size());
        runs.put(runId, state);
        (safe ? latestSafe : latestUnsafe).set(runId);
        (safe ? safeCoordinator : unsafeCoordinator)
                .submit(() -> coordinate(state, pendingFiles, safe));
        return runId;
    }

    public BatchProgress latest(String mode) {
        boolean safe = parseMode(mode);
        UUID runId = (safe ? latestSafe : latestUnsafe).get();
        if (runId == null) {
            throw new NoSuchElementException("No " + mode + " collector run exists");
        }
        return progress(runId);
    }

    public List<SourceFileStatus> latestFiles(String mode) {
        boolean safe = parseMode(mode);
        UUID runId = (safe ? latestSafe : latestUnsafe).get();
        if (runId == null) {
            throw new NoSuchElementException("No " + mode + " collector run exists");
        }
        return files(runId);
    }

    public BatchProgress progress(UUID runId) {
        BatchLabState state = runs.get(runId);
        if (state == null) {
            throw new NoSuchElementException("Unknown collector run: " + runId);
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

    public List<SourceFileStatus> files(UUID runId) {
        return jdbcTemplate.query("""
                SELECT id, collector_run_id, file_name, file_path, status, error_message
                FROM source_files WHERE collector_run_id = ? ORDER BY created_at, file_name
                """, (rs, row) -> new SourceFileStatus(
                rs.getObject("id", UUID.class), rs.getObject("collector_run_id", UUID.class),
                rs.getString("file_name"), rs.getString("file_path"),
                rs.getString("status"), rs.getString("error_message")), runId);
    }

    private void coordinate(BatchLabState state, List<SourceFileStatus> files, boolean safe) {
        CountDownLatch latch = new CountDownLatch(files.size());
        state.latch = latch;
        files.forEach(file -> workers.submit(() -> process(state, latch, file, safe)));
        try {
            boolean completed = safe ? latch.await(SAFE_TIMEOUT_SECONDS, TimeUnit.SECONDS) : awaitForever(latch);
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

    private boolean awaitForever(CountDownLatch latch) throws InterruptedException {
        latch.await();
        return true;
    }

    private void process(BatchLabState state, CountDownLatch latch,
                         SourceFileStatus file, boolean safe) {
        try {
            jdbcTemplate.update("UPDATE source_files SET status = 'PROCESSING' WHERE id = ?", file.id());
            if (!fileNamePolicy.isValid(file.fileName())) {
                rejectInvalid(state, file);
                if (!safe) {
                    // Intentional reproduction: this return bypasses the signal below.
                    return;
                }
            } else {
                try (InputStream input = Files.newInputStream(Path.of(file.filePath()))) {
                    IngestionResult result = ingestionService.ingest(file.fileName(), input);
                    state.parsedRows.addAndGet(result.successfulRows());
                    state.successfulFiles.incrementAndGet();
                    jdbcTemplate.update("""
                            UPDATE source_files SET status = 'COMPLETED', completed_at = NOW()
                            WHERE id = ?
                            """, file.id());
                }
            }
        } catch (RuntimeException | IOException exception) {
            state.failedFiles.incrementAndGet();
            state.lastError = exception.getMessage();
            jdbcTemplate.update("""
                    UPDATE source_files SET status = 'FAILED', error_message = ?, completed_at = NOW()
                    WHERE id = ?
                    """, exception.getMessage(), file.id());
        } finally {
            if (safe) {
                signal(state, latch);
            }
        }
        if (!safe) {
            signal(state, latch);
        }
    }

    private void rejectInvalid(BatchLabState state, SourceFileStatus file) {
        String error = "Invalid filename; expected UUID prefix: " + file.fileName();
        state.invalidFiles.incrementAndGet();
        state.lastError = error;
        jdbcTemplate.update("""
                UPDATE source_files SET status = 'INVALID', error_message = ?, completed_at = NOW()
                WHERE id = ?
                """, error, file.id());
    }

    private void signal(BatchLabState state, CountDownLatch latch) {
        state.completionSignals.incrementAndGet();
        latch.countDown();
    }

    private boolean parseMode(String mode) {
        if ("safe".equalsIgnoreCase(mode)) return true;
        if ("unsafe".equalsIgnoreCase(mode)) return false;
        throw new IllegalArgumentException("Mode must be 'unsafe' or 'safe'");
    }
}
