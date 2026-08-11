package dev.learning.platform.ingestion;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class UuidPrefixedFileNamePolicy {
    private static final Pattern UUID_PREFIXED_CSV = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
                    + "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}-.+\\.csv$");

    public boolean isValid(String fileName) {
        return fileName != null && UUID_PREFIXED_CSV.matcher(fileName).matches();
    }
}

