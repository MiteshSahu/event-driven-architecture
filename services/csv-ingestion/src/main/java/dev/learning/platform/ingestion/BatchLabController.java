package dev.learning.platform.ingestion;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/batch-lab")
public class BatchLabController {
    private final UnsafeFileBatchService batchService;
    private final SafeFileBatchService safeBatchService;

    public BatchLabController(UnsafeFileBatchService batchService, SafeFileBatchService safeBatchService) {
        this.batchService = batchService;
        this.safeBatchService = safeBatchService;
    }

    @PostMapping(path = "/jobs", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> submit(@RequestParam("files") List<MultipartFile> files) {
        UUID jobId = batchService.submit(files);
        return Map.of("jobId", jobId, "submittedFiles", files.size(), "status", "RUNNING");
    }

    @GetMapping("/jobs/latest")
    public BatchProgress latest() {
        return batchService.latestProgress();
    }

    @GetMapping("/jobs/{jobId}")
    public BatchProgress progress(@PathVariable UUID jobId) {
        return batchService.progress(jobId);
    }

    @PostMapping(path = "/safe/jobs", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> submitSafe(@RequestParam("files") List<MultipartFile> files) {
        UUID jobId = safeBatchService.submit(files);
        return Map.of("jobId", jobId, "submittedFiles", files.size(), "status", "RUNNING");
    }

    @GetMapping("/safe/jobs/latest")
    public BatchProgress latestSafe() {
        return safeBatchService.latestProgress();
    }

    @GetMapping("/safe/jobs/{jobId}")
    public BatchProgress safeProgress(@PathVariable UUID jobId) {
        return safeBatchService.progress(jobId);
    }
}
