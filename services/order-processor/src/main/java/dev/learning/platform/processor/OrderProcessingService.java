package dev.learning.platform.processor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderProcessingService {
    private final JdbcTemplate jdbcTemplate;

    public OrderProcessingService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean process(String topic, int partition, long offset, CdcOrderEvent event) {
        int newEvent = jdbcTemplate.update("""
                INSERT INTO processed_events
                    (topic, partition_id, offset_id, order_id, operation)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (topic, partition_id, offset_id) DO NOTHING
                """, topic, partition, offset, event.orderId(), event.operation());

        if (newEvent == 0) {
            return false;
        }

        if ("d".equals(event.operation())) {
            jdbcTemplate.update("DELETE FROM processed_orders WHERE order_id = ?", event.orderId());
            return true;
        }

        jdbcTemplate.update("""
                INSERT INTO processed_orders
                    (order_id, customer_id, product_id, amount, status,
                     source_operation, source_partition, source_offset, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    source_operation = EXCLUDED.source_operation,
                    source_partition = EXCLUDED.source_partition,
                    source_offset = EXCLUDED.source_offset,
                    processed_at = NOW()
                """, event.orderId(), event.customerId(), event.productId(), event.amount(),
                event.status(), event.operation(), partition, offset);
        return true;
    }
}

