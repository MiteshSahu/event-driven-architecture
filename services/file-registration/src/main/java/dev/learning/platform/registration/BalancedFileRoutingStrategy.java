package dev.learning.platform.registration;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class BalancedFileRoutingStrategy implements RoutingKeyStrategy {
    @Override
    public String name() {
        return "balanced";
    }

    @Override
    public UUID routingKey(UUID batchId, UUID fileId) {
        // Each file hashes independently, allowing Kafka to use all partitions.
        return fileId;
    }
}
