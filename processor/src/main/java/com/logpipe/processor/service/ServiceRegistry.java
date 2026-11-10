package com.logpipe.processor.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Set;

@Service
public class ServiceRegistry {

    private static final String KEY = "services";

    private final StringRedisTemplate stringRedisTemplate;

    public ServiceRegistry(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void recordSeen(String service, long timestampMillis) {
        stringRedisTemplate.opsForZSet().add(KEY, service, timestampMillis);
    }

    public Set<String> allServices() {
        Set<String> services = stringRedisTemplate.opsForZSet().range(KEY, 0, -1);
        return Objects.isNull(services) ? Set.of() : services;
    }
}
