package com.logpipe.processor.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

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
}
