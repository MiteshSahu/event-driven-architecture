package dev.learning.platform.collector;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Service
public class CsvFileProcessor {
    private final JdbcTemplate jdbcTemplate;
    private final String instanceId;

    public CsvFileProcessor(JdbcTemplate jdbcTemplate,
                            @Value("${HOSTNAME:unknown-collector}") String instanceId) {
        this.jdbcTemplate = jdbcTemplate;
        this.instanceId = instanceId;
    }

    @Transactional(rollbackFor = Exception.class)
    public long process(SourceFileEvent file) throws IOException {
        int claimed = jdbcTemplate.update("""
                UPDATE source_files SET status = 'PROCESSING', processed_by = ?
                WHERE id = ? AND status IN ('PENDING', 'RETRYING')
                """, instanceId, file.id());
        if (claimed == 0) {
            throw new IllegalStateException("File is not claimable: " + file.id());
        }

        UUID ingestionJobId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO ingestion_jobs (id, file_name, status, started_at)
                VALUES (?, ?, 'RUNNING', NOW())
                """, ingestionJobId, file.fileName());

        long rows = 0;
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader().setSkipHeaderRecord(true).setTrim(true).get();
        try (CSVParser parser = format.parse(new InputStreamReader(
                Files.newInputStream(Path.of(file.filePath())), StandardCharsets.UTF_8))) {
            for (CSVRecord record : parser) {
                insertOrder(ingestionJobId, record);
                rows++;
            }
        }

        jdbcTemplate.update("""
                UPDATE ingestion_jobs SET status = 'COMPLETED', total_rows = ?,
                    successful_rows = ?, completed_at = NOW() WHERE id = ?
                """, rows, rows, ingestionJobId);
        jdbcTemplate.update("""
                UPDATE source_files SET status = 'COMPLETED', completed_at = NOW()
                WHERE id = ?
                """, file.id());
        return rows;
    }

    public void markInvalid(SourceFileEvent file, String error) {
        jdbcTemplate.update("""
                UPDATE source_files SET status = 'INVALID', error_message = ?,
                    processed_by = ?, completed_at = NOW()
                WHERE id = ? AND status = 'PENDING'
                """, error, instanceId, file.id());
    }

    public void markFailed(SourceFileEvent file, String error) {
        jdbcTemplate.update("""
                UPDATE source_files SET status = 'FAILED', error_message = ?,
                    processed_by = ?, completed_at = NOW()
                WHERE id = ?
                """, error, instanceId, file.id());
    }

    public void markRetrying(SourceFileEvent file, String error) {
        jdbcTemplate.update("""
                UPDATE source_files SET status = 'RETRYING', error_message = ?
                WHERE id = ? AND status IN ('PENDING', 'RETRYING')
                """, error, file.id());
    }

    private void insertOrder(UUID jobId, CSVRecord record) {
        jdbcTemplate.update("""
                INSERT INTO orders
                    (order_id, customer_id, product_id, amount, status, event_time, ingestion_job_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (order_id) DO NOTHING
                """, required(record, "orderId"), required(record, "customerId"),
                required(record, "productId"), new BigDecimal(required(record, "amount")),
                required(record, "status"), Timestamp.from(Instant.parse(required(record, "eventTime"))),
                jobId);
    }

    private String required(CSVRecord record, String column) {
        String value = record.get(column);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(column + " is required");
        }
        return value;
    }
}
