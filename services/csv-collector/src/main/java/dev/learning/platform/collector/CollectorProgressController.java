package dev.learning.platform.collector;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/collector")
public class CollectorProgressController {
    private final EventDrivenCollector collector;

    public CollectorProgressController(EventDrivenCollector collector) {
        this.collector = collector;
    }

    @GetMapping("/batches/latest")
    public CollectorBatchProgress latest() {
        return collector.latest();
    }

    @GetMapping("/batches")
    public List<CollectorBatchProgress> all() {
        return collector.all();
    }
}
