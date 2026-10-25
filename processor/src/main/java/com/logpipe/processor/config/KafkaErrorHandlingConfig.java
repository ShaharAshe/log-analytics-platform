package com.logpipe.processor.config;

import com.logpipe.processor.dto.LogEvent;
import com.logpipe.processor.exception.InvalidLogEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public NewTopic deadLetterTopic(@Value("${logpipe.topic}") String topic,
                                    @Value("${logpipe.partitions}") int partitions) {
        return TopicBuilder.name(topic + "-dlt").partitions(partitions).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, byte[]> byteTypeTemplate,
                                            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(LogEvent.class, logEventTemplate(bootstrapServers));
        templates.put(byte[].class, byteTypeTemplate);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates);

        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
        defaultErrorHandler.setBackOffFunction((record, exception) ->
                isInfrastructureFailure(exception) ?
                        new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS)
                        :
                        null
        );
        defaultErrorHandler.addNotRetryableExceptions(InvalidLogEventException.class);
        return defaultErrorHandler;
    }

    private static KafkaTemplate<String, LogEvent> logEventTemplate(String bootstrapServers) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                "spring.json.add.type.headers", false);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
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
