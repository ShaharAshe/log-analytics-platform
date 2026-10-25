package com.logpipe.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Objects;

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
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
        defaultErrorHandler.setBackOffFunction((record, exception) ->
                isInfrastructureFailure(exception) ?
                        new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS)
                        :
                        null
        );
        return defaultErrorHandler;
    }

    private static boolean isInfrastructureFailure(Throwable exception) {
        Throwable curr = exception;
        while (Objects.nonNull(curr)) {
            if (curr instanceof RedisConnectionFailureException || curr instanceof QueryTimeoutException) {
                return true;
            }
            curr = curr.getCause();
        }
        return false;
    }
}
