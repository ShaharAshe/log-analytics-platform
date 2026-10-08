package com.logpipe.ingest.service;

import com.logpipe.ingest.dto.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
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
        try {
            CompletableFuture<SendResult<String, LogEvent>> sendResult = kafkaTemplate.send(topic, event.service(), event);
            sendResult
                    .whenComplete((result, exception) -> {
                        if (Objects.nonNull(exception)) {
                            logger.error("Kafka write failed for log event {} with traceId {}", event.service(), event.traceId(), exception);
                        } else {
                            logger.debug("Log event sent successfully: partition={}, offset={}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                        }
                    });
        } catch (KafkaException e) {
            logger.error("Kafka unavailable, dropping log event {} with traceId {}", event.service(), event.traceId(), e);
        }
    }
}
