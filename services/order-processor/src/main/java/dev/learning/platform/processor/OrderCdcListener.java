package dev.learning.platform.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderCdcListener {
    private static final Logger log = LoggerFactory.getLogger(OrderCdcListener.class);

    private final ObjectMapper objectMapper;
    private final OrderProcessingService processingService;
    private final UnsafeSequenceCoordinator sequenceCoordinator;
    private final long processingDelayMs;

    public OrderCdcListener(
            ObjectMapper objectMapper,
            OrderProcessingService processingService,
            UnsafeSequenceCoordinator sequenceCoordinator,
            @Value("${app.processing.delay-ms:25}") long processingDelayMs,
            @Value("${spring.kafka.listener.concurrency:1}") int consumerConcurrency) {
        this.objectMapper = objectMapper;
        this.processingService = processingService;
        this.sequenceCoordinator = sequenceCoordinator;
        this.processingDelayMs = processingDelayMs;
        log.info("Configured consumer concurrency={} processingDelayMs={}",
                consumerConcurrency, processingDelayMs);
    }

    @KafkaListener(topics = "orders-cdc.public.orders")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
            throws JsonProcessingException {
        JsonNode envelope = objectMapper.readTree(record.value());
        CdcOrderEvent event = CdcOrderEvent.from(envelope);
        int sequence = sequenceCoordinator.enabled() ? sequenceFrom(event.orderId()) : -1;

        log.info("Received order={} operation={} partition={} offset={} thread={}",
                event.orderId(), event.operation(), record.partition(), record.offset(),
                Thread.currentThread().getName());

        sequenceCoordinator.awaitTurn(sequence);
        simulateBusinessWork();

        boolean processed = processingService.process(
                record.topic(), record.partition(), record.offset(), event);

        sequenceCoordinator.markCompleted(sequence);

        acknowledgment.acknowledge();
        log.info("Acknowledged partition={} offset={} result={}",
                record.partition(), record.offset(), processed ? "processed" : "duplicate");
    }

    private int sequenceFrom(String orderId) {
        int separator = orderId.lastIndexOf('-');
        if (separator < 0 || separator == orderId.length() - 1) {
            throw new IllegalArgumentException(
                    "Unsafe sequence lab requires an orderId ending in a numeric sequence: " + orderId);
        }
        return Integer.parseInt(orderId.substring(separator + 1));
    }

    private void simulateBusinessWork() {
        if (processingDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(processingDelayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consumer processing was interrupted", exception);
        }
    }
}
