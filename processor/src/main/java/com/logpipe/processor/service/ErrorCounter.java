package com.logpipe.processor.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class ErrorCounter {

    private static final String KEY_PREFIX = "error:";
    private final StringRedisTemplate stringRedisTemplate;

    public ErrorCounter(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long increment(String service) {
        return stringRedisTemplate.opsForValue().increment(KEY_PREFIX + service);
    }
}
