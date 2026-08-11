package dev.learning.platform.ingestion;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/collector")
public class CsvCollectorController {
    private final CsvCollectorService collectorService;

    public CsvCollectorController(CsvCollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @PostMapping("/runs/{mode}")
    public Map<String, Object> start(@PathVariable String mode,
                                     @RequestBody CollectorRunRequest request) {
        UUID runId = collectorService.start(mode, request.files());
        return Map.of("runId", runId, "mode", mode, "status", "RUNNING");
    }

    @GetMapping("/runs/{mode}/latest")
    public BatchProgress latest(@PathVariable String mode) {
        return collectorService.latest(mode);
    }

    @GetMapping("/runs/{mode}/latest/files")
    public List<SourceFileStatus> latestFiles(@PathVariable String mode) {
        return collectorService.latestFiles(mode);
    }

    @GetMapping("/runs/id/{runId}")
    public BatchProgress progress(@PathVariable UUID runId) {
        return collectorService.progress(runId);
    }

    @GetMapping("/runs/id/{runId}/files")
    public List<SourceFileStatus> files(@PathVariable UUID runId) {
        return collectorService.files(runId);
    }
}
