package com.logpipe.processor.listener;

import com.logpipe.processor.dto.Levels;
import com.logpipe.processor.dto.LogEvent;
import com.logpipe.processor.service.ErrorCounter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class RawLogListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ErrorCounter errorCounter;

    public RawLogListener(ErrorCounter errorCounter) {
        this.errorCounter = errorCounter;
    }

    @KafkaListener(topics = "${logpipe.topic}")
    public void listen(ConsumerRecord<String, LogEvent> record) {
        LogEvent logEvent = record.value();
        logger.info("Partition: {} Offset: {} Key: {} Service: {} Level: {} Message: {} TraceId: {} DurationMs: {}",
                record.partition(), record.offset(), record.key(), logEvent.service(), logEvent.level(), logEvent.message(), logEvent.traceId(), logEvent.durationMs());
        if (Objects.equals(logEvent.level(), Levels.ERROR.name())) {
            long countServiceErrors = errorCounter.increment(logEvent.service());
            logger.info("Errors count for service: {} are {}", logEvent.service(), countServiceErrors);
        }
    }
}
