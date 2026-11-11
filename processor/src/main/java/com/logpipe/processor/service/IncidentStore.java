package com.logpipe.processor.service;

import com.logpipe.processor.dto.Incident;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class IncidentStore {

    private static final String KEY_PREFIX = "incident:";
    private static final String ID = "id";
    private static final String OPENED_AT = "openedAt";

    private final StringRedisTemplate stringRedisTemplate;

    public IncidentStore(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public Optional<Incident> findOpen(String service) {
        HashOperations<String, String, String> hash = stringRedisTemplate.opsForHash();
        Map<String, String> fields = hash.entries(KEY_PREFIX + service);
        if (Objects.isNull(fields) || fields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Incident(fields.get(ID), Long.parseLong(fields.get(OPENED_AT))));
    }

    public Incident open(String service) {
        Incident incident = new Incident(UUID.randomUUID().toString(), System.currentTimeMillis());
        HashOperations<String, String, String> hash = stringRedisTemplate.opsForHash();
        hash.putAll(KEY_PREFIX + service,
                Map.of(ID, incident.id(),
                        OPENED_AT, String.valueOf(incident.openedAt())));
        return incident;
    }
}
