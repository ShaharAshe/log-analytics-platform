package com.logpipe.processor.listener;

import com.logpipe.processor.dto.Level;
import com.logpipe.processor.dto.LogEvent;
import com.logpipe.processor.exception.InvalidLogEventException;
import com.logpipe.processor.service.ErrorCounter;
import com.logpipe.processor.service.ErrorWindow;
import com.logpipe.processor.service.ServiceRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class RawLogListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ServiceRegistry serviceRegistry;
    private final ErrorCounter errorCounter;
    private final ErrorWindow errorWindow;

    public RawLogListener(ServiceRegistry serviceRegistry, ErrorCounter errorCounter, ErrorWindow errorWindow) {
        this.serviceRegistry = serviceRegistry;
        this.errorCounter = errorCounter;
        this.errorWindow = errorWindow;
    }

    @KafkaListener(topics = "${logpipe.topic}")
    public void listen(ConsumerRecord<String, LogEvent> record) {
        LogEvent logEvent = record.value();
        if (Objects.isNull(logEvent.service()) || logEvent.service().isBlank()) {
            String message = String.format(
                    "Log event has no service: partition=%d, offset=%d",
                    record.partition(),
                    record.offset()
            );
            throw new InvalidLogEventException(message);
        }
        logger.info("Partition: {} Offset: {} Key: {} Service: {} Level: {} Message: {} TraceId: {} DurationMs: {}",
                record.partition(), record.offset(), record.key(), logEvent.service(), logEvent.level(),
                logEvent.message(), logEvent.traceId(), logEvent.durationMs());
        serviceRegistry.recordSeen(logEvent.service(), record.timestamp());
        if (Objects.equals(logEvent.level(), Level.ERROR.name())) {
            long countServiceErrors = errorCounter.increment(logEvent.service());
            logger.info("Errors count for service: {} are {}", logEvent.service(), countServiceErrors);
            long errorsLastMinute = errorWindow.record(logEvent.service(), record.partition(), record.offset(), record.timestamp());
            logger.info("Errors in the last minute for {}: {}", logEvent.service(), errorsLastMinute);
        }
    }
}
