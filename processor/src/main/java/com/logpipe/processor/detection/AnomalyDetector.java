package com.logpipe.processor.detection;

import com.logpipe.processor.config.DetectionProperties;
import com.logpipe.processor.dto.Incident;
import com.logpipe.processor.dto.IncidentAction;
import com.logpipe.processor.service.IncidentStore;
import com.logpipe.processor.service.ErrorWindow;
import com.logpipe.processor.service.ServiceRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Component
public class AnomalyDetector {

    private final Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);

    private final ServiceRegistry serviceRegistry;
    private final ErrorWindow errorWindow;
    private final DetectionProperties properties;
    private final IncidentStore incidentStore;

    public AnomalyDetector(ServiceRegistry serviceRegistry,
                           ErrorWindow errorWindow,
                           DetectionProperties properties,
                           IncidentStore incidentStore) {
        this.serviceRegistry = serviceRegistry;
        this.errorWindow = errorWindow;
        this.properties = properties;
        this.incidentStore = incidentStore;
    }

    @Scheduled(fixedDelayString = "${logpipe.detection.check-interval}")
    @SchedulerLock(name = "anomaly-detector", lockAtMostFor = "PT1M", lockAtLeastFor = "PT5S")
    public void check() {
        long nowMillis = System.currentTimeMillis();
        Duration resolveAfter = properties.resolveAfter();
        for (String service : serviceRegistry.allServices()) {
            long errors = errorWindow.countRecent(service);
            boolean aboveThreshold = errors >= properties.minErrors();
            Optional<Incident> openIncident = incidentStore.findOpen(service);
            IncidentAction action = IncidentDecider.decide(aboveThreshold, openIncident, nowMillis, resolveAfter);
            switch (action) {
                case OPEN -> {
                    Incident incident = incidentStore.open(service);
                    logger.warn("FIRING {}: {} errors in the last {}s, incident {}",
                            service, errors, properties.window().toSeconds(), incident.id());
                }
                case START_BELOW -> {
                    incidentStore.markBelow(service, nowMillis);
                    logger.info("BELOW_THRESHOLD : the service {} just dropped below the threshold : {} errors in the last {}s, incident {}",
                            service, errors, properties.window().toSeconds(), openIncident.get().id());
                }
                case RESET_BELOW -> {
                    incidentStore.clearBelow(service);
                    logger.info("ABOVE_THRESHOLD : the service {} went back above the threshold : {} errors in the last {}s, incident {}",
                            service, errors, properties.window().toSeconds(), openIncident.get().id());
                }
                case RESOLVE -> {
                    incidentStore.resolve(service);
                    Incident incident = openIncident.get();
                    long durationSeconds = Duration.ofMillis(nowMillis - incident.openedAt()).toSeconds();
                    logger.info("RESOLVED {}: after {}s, {} errors in the last {}s, incident {}",
                            service, durationSeconds, errors, properties.window().toSeconds(), incident.id());
                }
                case NONE -> {
                }
            }
        }
    }
}
