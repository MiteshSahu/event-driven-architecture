package dev.learning.platform.ingestion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UuidPrefixedFileNamePolicyTest {
    private final UuidPrefixedFileNamePolicy policy = new UuidPrefixedFileNamePolicy();

    @Test
    void acceptsUuidPrefixedCsv() {
        assertThat(policy.isValid(
                "550e8400-e29b-41d4-a716-446655440000-orders.csv")).isTrue();
    }

    @Test
    void rejectsCsvWithoutUuidPrefix() {
        assertThat(policy.isValid("invalid-orders.csv")).isFalse();
    }
}

