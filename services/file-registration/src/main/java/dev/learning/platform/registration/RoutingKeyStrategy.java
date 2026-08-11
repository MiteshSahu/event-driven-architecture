package dev.learning.platform.registration;

import java.util.UUID;

public interface RoutingKeyStrategy {
    String name();

    UUID routingKey(UUID batchId, UUID fileId);
}
