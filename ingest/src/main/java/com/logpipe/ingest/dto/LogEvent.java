package com.logpipe.ingest.dto;

import lombok.NonNull;

import java.time.Instant;

public record LogEvent(Instant timestamp,
                       @NonNull String service,
                       @NonNull String level,
                       @NonNull String message,
                       String traceId,
                       Long durationMs) {
}
