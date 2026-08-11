package dev.learning.platform.collector;

public record CollectorProcessingResult(boolean deadLetterRequired, String error) {
    static CollectorProcessingResult completed() {
        return new CollectorProcessingResult(false, null);
    }

    static CollectorProcessingResult deadLetter(String error) {
        return new CollectorProcessingResult(true, error);
    }
}
