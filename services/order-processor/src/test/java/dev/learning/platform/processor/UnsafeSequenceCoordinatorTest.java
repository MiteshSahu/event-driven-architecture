package dev.learning.platform.processor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UnsafeSequenceCoordinatorTest {
    @Test
    void intentionallySkipsTheConfiguredNextSequence() {
        UnsafeSequenceCoordinator coordinator =
                new UnsafeSequenceCoordinator("unsafe-sequence", 49, 49);

        coordinator.awaitTurn(49);
        coordinator.markCompleted(49);

        assertThat(coordinator.nextExpected()).isEqualTo(51);
        assertThat(coordinator.faultTriggered()).isTrue();
    }
}

