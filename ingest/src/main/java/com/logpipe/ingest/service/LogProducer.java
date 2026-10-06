package com.logpipe.ingest.service;

import com.logpipe.ingest.dto.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
public class LogProducer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final KafkaTemplate<String, LogEvent> kafkaTemplate;
    private final String topic;
    public LogProducer(KafkaTemplate<String, LogEvent> kafkaTemplate,
                       @Value("${logpipe.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(LogEvent event) {
        CompletableFuture<SendResult<String, LogEvent>> sendResult = kafkaTemplate.send(topic, event.service(), event);
        sendResult
                .whenComplete((result, exception) -> {
                    if (Objects.nonNull(exception)) {
                        logger.error("Failed to send log event", exception);
                    }
                    if (Objects.nonNull(result)) {
                        logger.debug("Log event sent successfully: partition={}, offset={}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    } else {
                        logger.debug("Log event sent successfully");
                    }
                });
    }
}
