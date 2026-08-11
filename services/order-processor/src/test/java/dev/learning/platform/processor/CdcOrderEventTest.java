package dev.learning.platform.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class CdcOrderEventTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesDebeziumCreateEnvelope() throws Exception {
        var envelope = objectMapper.readTree("""
                {
                  "before": null,
                  "after": {
                    "order_id": "order-1",
                    "customer_id": "customer-1",
                    "product_id": "product-1",
                    "amount": "42.50",
                    "status": "CREATED"
                  },
                  "op": "c"
                }
                """);

        CdcOrderEvent event = CdcOrderEvent.from(envelope);

        assertThat(event.orderId()).isEqualTo("order-1");
        assertThat(event.operation()).isEqualTo("c");
        assertThat(event.amount()).isEqualByComparingTo(new BigDecimal("42.50"));
    }
}

