package com.logpipe.processor.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RawLogListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = "${logpipe.topic}")
    public void listen(ConsumerRecord<String, String> record) {
        logger.info("Partition: {} Offset: {} Key: {} Value: {}", record.partition(), record.offset(), record.key(), record.value());
    }
}
