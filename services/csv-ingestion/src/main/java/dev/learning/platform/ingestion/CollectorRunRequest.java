package dev.learning.platform.ingestion;

import java.util.List;

public record CollectorRunRequest(List<SourceFileRegistration> files) {
}
