package dev.learning.platform.ingestion;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/ingestions")
public class CsvIngestionController {
    private final CsvIngestionService ingestionService;

    public CsvIngestionController(CsvIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @GetMapping
    public Map<String, String> instructions() {
        return Map.of(
                "service", "csv-ingestion",
                "usage", "POST a multipart CSV file to /api/ingestions/csv using field name 'file'"
        );
    }

    @PostMapping(path = "/csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public IngestionResult ingest(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            throw new IllegalArgumentException("CSV file must not be empty");
        }
        return ingestionService.ingest(file.getOriginalFilename(), file.getInputStream());
    }
}

