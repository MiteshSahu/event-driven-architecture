package dev.learning.platform.processor;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/consumer-progress")
public class ConsumerProgressController {
    private final UnsafeSequenceCoordinator coordinator;

    public ConsumerProgressController(UnsafeSequenceCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @GetMapping
    public Map<String, Object> progress() {
        long secondsSinceProgress = Duration.between(
                coordinator.lastProgressAt(), Instant.now()).toSeconds();
        boolean stuck = coordinator.enabled()
                && coordinator.waitingThreads() > 0
                && secondsSinceProgress >= 10;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", coordinator.enabled() ? "unsafe-sequence" : "safe-partition");
        result.put("nextExpectedSequence", coordinator.nextExpected());
        result.put("completedSequences", coordinator.completedSequences());
        result.put("waitingThreads", coordinator.waitingThreads());
        result.put("faultTriggered", coordinator.faultTriggered());
        result.put("lastProgressAt", coordinator.lastProgressAt());
        result.put("secondsSinceProgress", secondsSinceProgress);
        result.put("stuck", stuck);
        return result;
    }
}

