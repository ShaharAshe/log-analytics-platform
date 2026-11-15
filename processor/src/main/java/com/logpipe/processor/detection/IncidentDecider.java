package com.logpipe.processor.detection;

import com.logpipe.processor.dto.Incident;
import com.logpipe.processor.dto.IncidentAction;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public final class IncidentDecider {

    private IncidentDecider() {
    }

    public static IncidentAction decide(boolean aboveThreshold,
                                        Optional<Incident> openIncident,
                                        long nowMillis,
                                        Duration resolveAfter) {
        if (openIncident.isEmpty()) {
            return aboveThreshold ? IncidentAction.OPEN : IncidentAction.NONE;
        }
        Long belowSince = openIncident.get().belowSince();
        boolean belowSinceExists = Objects.nonNull(belowSince);
        if (aboveThreshold) {
            return belowSinceExists ? IncidentAction.RESET_BELOW : IncidentAction.NONE;
        }
        if (!belowSinceExists) {
            return IncidentAction.START_BELOW;
        }
        return nowMillis - belowSince >= resolveAfter.toMillis() ? IncidentAction.RESOLVE : IncidentAction.NONE;
    }
}
