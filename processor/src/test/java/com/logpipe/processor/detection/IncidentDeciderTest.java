package com.logpipe.processor.detection;

import com.logpipe.processor.dto.Incident;
import com.logpipe.processor.dto.IncidentAction;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IncidentDeciderTest {

    private static final Duration RESOLVE_AFTER = Duration.ofSeconds(30L);
    private static final long NOW_MILLIS = 1_000_000L;

    @Test
    void doesNothingWhenBelowThresholdAndNoOpenIncident() {
        boolean aboveThreshold = false;
        Optional<Incident> openIncident = Optional.empty();

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.NONE);
    }

    @Test
    void opensIncidentWhenAboveThresholdAndNoOpenIncident() {
        boolean aboveThreshold = true;
        Optional<Incident> openIncident = Optional.empty();

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.OPEN);
    }

    @Test
    void doesNothingWhenAboveThresholdAndOpenIncidentAndNoBelowSince() {
        boolean aboveThreshold = true;
        Optional<Incident> openIncident = createIncident(null);

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.NONE);
    }

    @Test
    void resetsBelowSinceWhenAboveThresholdAndOpenIncidentAndBelowSince() {
        boolean aboveThreshold = true;
        Optional<Incident> openIncident = createIncident(NOW_MILLIS);

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.RESET_BELOW);
    }

    @Test
    void startsBelowSinceWhenBelowThresholdAndOpenIncidentAndNoBelowSince() {
        boolean aboveThreshold = false;
        Optional<Incident> openIncident = createIncident(null);

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.START_BELOW);
    }

    @Test
    void doesNothingWhenBelowThresholdAndOpenIncidentAndBelowSinceBelowResolveTime() {
        boolean aboveThreshold = false;
        long belowSince = NOW_MILLIS - 29_999L;
        Optional<Incident> openIncident = createIncident(belowSince);

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.NONE);
    }

    @Test
    void resolvesIncidentWhenBelowThresholdAndOpenIncidentAndBelowSinceForExactlyResolveAfter() {
        boolean aboveThreshold = false;
        long belowSince = NOW_MILLIS - 30_000L;
        Optional<Incident> openIncident = createIncident(belowSince);

        IncidentAction actual = IncidentDecider.decide(aboveThreshold, openIncident, NOW_MILLIS, RESOLVE_AFTER);

        assertThat(actual).isEqualTo(IncidentAction.RESOLVE);
    }

    private Optional<Incident> createIncident(Long belowSince) {
        return Optional.of(new Incident("Incident-1", 0L, belowSince));
    }
}
