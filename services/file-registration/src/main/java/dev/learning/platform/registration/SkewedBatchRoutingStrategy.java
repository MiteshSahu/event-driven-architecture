package dev.learning.platform.registration;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class SkewedBatchRoutingStrategy implements RoutingKeyStrategy {
    @Override
    public String name() {
        return "skewed";
    }

    @Override
    public UUID routingKey(UUID batchId, UUID fileId) {
        // INTENTIONAL LAB ISSUE: every file in the batch gets the same Kafka key.
        return batchId;
    }
}
