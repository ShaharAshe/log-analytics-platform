package com.logpipe.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = DetectionProperties.PREFIX)
public record DetectionProperties(
        Duration window,
        Duration checkInterval,
        Integer minErrors,
        Double spikeFactor,
        Double ewmaAlpha,
        Duration resolveAfter
) {

    public static final String PREFIX = "logpipe.detection";

    public DetectionProperties {
        require(Objects.nonNull(window) && window.compareTo(Duration.ZERO) > 0, "window must be positive");
        require(
                Objects.nonNull(checkInterval) && checkInterval.compareTo(Duration.ZERO) > 0,
                "check-interval must be positive"
        );
        require(Objects.nonNull(minErrors) && minErrors > 0, "min-errors must be positive");
        require(
                Objects.nonNull(spikeFactor) && spikeFactor > 1,
                "spike-factor must be greater than 1"
        );
        require(
                Objects.nonNull(ewmaAlpha) && ewmaAlpha > 0 && ewmaAlpha <= 1,
                "ewma-alpha must be in (0, 1]"
        );
        require(
                Objects.nonNull(resolveAfter) && resolveAfter.compareTo(checkInterval) >= 0,
                "resolve-after must be at least check-interval"
        );
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(PREFIX + "." + message);
        }
    }
}
