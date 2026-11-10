package com.logpipe.processor.detection;

import com.logpipe.processor.config.DetectionProperties;
import com.logpipe.processor.service.ErrorWindow;
import com.logpipe.processor.service.ServiceRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AnomalyDetector {

    private final Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);

    private final ServiceRegistry serviceRegistry;
    private final ErrorWindow errorWindow;
    private final DetectionProperties properties;

    public AnomalyDetector(ServiceRegistry serviceRegistry, ErrorWindow errorWindow, DetectionProperties properties) {
        this.serviceRegistry = serviceRegistry;
        this.errorWindow = errorWindow;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${logpipe.detection.check-interval}")
    @SchedulerLock(name = "anomaly-detector", lockAtMostFor = "PT1M", lockAtLeastFor = "PT5S")
    public void check() {
        for (String service : serviceRegistry.allServices()) {
            long errors = errorWindow.countRecent(service);
            if (errors >= properties.minErrors()) {
                logger.warn("Error spike for {}: {} errors in the last {}", service, errors, properties.window());
            }
        }
    }
}
