package dev.learning.platform.ingestion;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Service
public class CsvIngestionService {
    private final JdbcTemplate jdbcTemplate;

    public CsvIngestionService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public IngestionResult ingest(String fileName, InputStream inputStream) throws IOException {
        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO ingestion_jobs (id, file_name, status, started_at)
                VALUES (?, ?, 'RUNNING', NOW())
                """, jobId, fileName);

        long total = 0;
        long successful = 0;
        long failed = 0;

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setTrim(true)
                .get();

        try (CSVParser parser = format.parse(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            for (CSVRecord record : parser) {
                total++;
                try {
                    insertOrder(jobId, record);
                    successful++;
                } catch (RuntimeException exception) {
                    failed++;
                }
            }
        }

        String status = failed == 0 ? "COMPLETED" : "COMPLETED_WITH_ERRORS";
        jdbcTemplate.update("""
                UPDATE ingestion_jobs
                SET status = ?, total_rows = ?, successful_rows = ?, failed_rows = ?, completed_at = NOW()
                WHERE id = ?
                """, status, total, successful, failed, jobId);

        return new IngestionResult(jobId, status, total, successful, failed);
    }

    private void insertOrder(UUID jobId, CSVRecord record) {
        String orderId = required(record, "orderId");
        String customerId = required(record, "customerId");
        String productId = required(record, "productId");
        String status = required(record, "status");
        BigDecimal amount = new BigDecimal(required(record, "amount"));
        Instant eventTime = Instant.parse(required(record, "eventTime"));

        int inserted = jdbcTemplate.update("""
                INSERT INTO orders
                    (order_id, customer_id, product_id, amount, status, event_time, ingestion_job_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (order_id) DO NOTHING
                """, orderId, customerId, productId, amount, status,
                Timestamp.from(eventTime), jobId);
        if (inserted == 0) {
            throw new IllegalArgumentException("duplicate orderId: " + orderId);
        }
    }

    private String required(CSVRecord record, String column) {
        String value = record.get(column);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(column + " is required");
        }
        return value;
    }
}
