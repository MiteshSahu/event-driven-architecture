package dev.learning.platform.processor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/processed-orders")
public class ProcessedOrderController {
    private final JdbcTemplate jdbcTemplate;

    public ProcessedOrderController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping
    public List<Map<String, Object>> findAll() {
        return jdbcTemplate.queryForList("""
                SELECT order_id, customer_id, product_id, amount, status,
                       source_operation, source_partition, source_offset, processed_at
                FROM processed_orders
                ORDER BY order_id
                """);
    }
}

