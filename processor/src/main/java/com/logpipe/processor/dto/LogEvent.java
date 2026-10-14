package com.logpipe.processor.dto;

import java.time.Instant;

public record LogEvent(Instant timestamp,
                       String service,
                       String level,
                       String message,
                       String traceId,
                       Long durationMs) {
}
