package dev.learning.platform.registration;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/source-files")
public class FileRegistrationController {
    private final JdbcTemplate jdbcTemplate;
    private final Map<String, RoutingKeyStrategy> routingStrategies;

    public FileRegistrationController(JdbcTemplate jdbcTemplate,
                                      List<RoutingKeyStrategy> routingStrategies) {
        this.jdbcTemplate = jdbcTemplate;
        this.routingStrategies = routingStrategies.stream().collect(
                java.util.stream.Collectors.toMap(RoutingKeyStrategy::name, strategy -> strategy));
    }

    @PostMapping("/batches")
    @Transactional
    public Map<String, Object> register(@RequestBody RegistrationRequest request) {
        if (request.files() == null || request.files().isEmpty()) {
            throw new IllegalArgumentException("At least one file is required");
        }
        UUID batchId = UUID.randomUUID();
        int expectedCount = request.files().size();
        request.files().forEach(file -> {
            UUID fileId = fileId();
            jdbcTemplate.update("""
                INSERT INTO source_files
                    (id, collector_run_id, routing_key, expected_file_count,
                     file_name, file_path, status)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                """, fileId, batchId,
                    routingStrategies.get("balanced").routingKey(batchId, fileId),
                    expectedCount, file.fileName(), file.filePath());
        });
        return Map.of("batchId", batchId, "registeredFiles", expectedCount,
                "message", "Committed to PostgreSQL; Debezium will emit the events");
    }

    @PostMapping("/batches/skewed")
    @Transactional
    public Map<String, Object> registerSkewed(@RequestParam(defaultValue = "60") int files) {
        if (files < 1 || files > 10000) {
            throw new IllegalArgumentException("files must be between 1 and 10000");
        }
        UUID batchId = UUID.randomUUID();
        for (int index = 0; index < files; index++) {
            UUID fileId = fileId();
            jdbcTemplate.update("""
                    INSERT INTO source_files
                        (id, collector_run_id, routing_key, expected_file_count,
                         file_name, file_path, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                    """, fileId, batchId,
                    routingStrategies.get("skewed").routingKey(batchId, fileId), files,
                    "550e8400-e29b-41d4-a716-446655440000-orders.csv",
                    "/data/file-batch/550e8400-e29b-41d4-a716-446655440000-orders.csv");
        }
        return Map.of("batchId", batchId, "registeredFiles", files,
                "message", "All rows share collector_run_id, producing one Kafka key");
    }

    @PostMapping("/batches/balanced")
    @Transactional
    public Map<String, Object> registerBalanced(@RequestParam(defaultValue = "60") int files) {
        if (files < 1 || files > 10000) {
            throw new IllegalArgumentException("files must be between 1 and 10000");
        }
        UUID batchId = UUID.randomUUID();
        for (int index = 0; index < files; index++) {
            UUID fileId = fileId();
            jdbcTemplate.update("""
                    INSERT INTO source_files
                        (id, collector_run_id, routing_key, expected_file_count,
                         file_name, file_path, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                    """, fileId, batchId,
                    routingStrategies.get("balanced").routingKey(batchId, fileId), files,
                    "550e8400-e29b-41d4-a716-446655440000-orders.csv",
                    "/data/file-batch/550e8400-e29b-41d4-a716-446655440000-orders.csv");
        }
        return Map.of("batchId", batchId, "registeredFiles", files,
                "message", "Each row has a unique routing key for Kafka partition distribution");
    }

    private UUID fileId() {
        return UUID.randomUUID();
    }

    @GetMapping("/batches/{batchId}")
    public List<Map<String, Object>> batch(@PathVariable UUID batchId) {
        return jdbcTemplate.queryForList("""
                SELECT file_name, file_path, status, error_message, processed_by, completed_at
                FROM source_files WHERE collector_run_id = ? ORDER BY created_at, file_name
                """, batchId);
    }

    @GetMapping("/batches/latest")
    public List<Map<String, Object>> latestBatch() {
        return jdbcTemplate.queryForList("""
                SELECT file_name, file_path, status, error_message, processed_by, completed_at
                FROM source_files
                WHERE collector_run_id = (
                    SELECT collector_run_id FROM source_files ORDER BY created_at DESC LIMIT 1
                )
                ORDER BY created_at, file_name
                """);
    }

    @GetMapping("/batches/latest/distribution")
    public List<Map<String, Object>> latestDistribution() {
        return jdbcTemplate.queryForList("""
                SELECT COALESCE(processed_by, 'UNPROCESSED') AS pod,
                       status, COUNT(*) AS files
                FROM source_files
                WHERE collector_run_id = (
                    SELECT collector_run_id FROM source_files ORDER BY created_at DESC LIMIT 1
                )
                GROUP BY processed_by, status
                ORDER BY files DESC, pod
                """);
    }

    public record RegistrationRequest(List<FileRegistration> files) {}
    public record FileRegistration(String fileName, String filePath) {}
}
