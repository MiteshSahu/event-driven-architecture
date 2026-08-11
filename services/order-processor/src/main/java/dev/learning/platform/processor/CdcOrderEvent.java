package dev.learning.platform.processor;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;

public record CdcOrderEvent(
        String operation,
        String orderId,
        String customerId,
        String productId,
        BigDecimal amount,
        String status
) {
    static CdcOrderEvent from(JsonNode envelope) {
        String operation = requiredText(envelope, "op");
        JsonNode row = "d".equals(operation) ? envelope.path("before") : envelope.path("after");
        if (row.isMissingNode() || row.isNull()) {
            throw new IllegalArgumentException("CDC event does not contain a usable row image");
        }

        String orderId = requiredText(row, "order_id");
        if ("d".equals(operation)) {
            return new CdcOrderEvent(operation, orderId, null, null, null, null);
        }

        String amountText = requiredText(row, "amount");
        return new CdcOrderEvent(
                operation,
                orderId,
                requiredText(row, "customer_id"),
                requiredText(row, "product_id"),
                new BigDecimal(amountText),
                requiredText(row, "status")
        );
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull() || value.asText().isBlank()) {
            throw new IllegalArgumentException("Missing required CDC field: " + field);
        }
        return value.asText();
    }
}

