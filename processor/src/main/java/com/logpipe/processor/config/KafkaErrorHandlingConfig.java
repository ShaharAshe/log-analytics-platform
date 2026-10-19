package com.logpipe.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public NewTopic deadLetterTopic(@Value("${logpipe.topic}") String topic,
                                    @Value("${logpipe.partitions}") int partitions) {
        return TopicBuilder.name(topic + "-dlt").partitions(partitions).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, byte[]> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
