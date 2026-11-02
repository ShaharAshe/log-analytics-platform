package com.logpipe.processor.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ErrorWindow {

    private static final String KEY_PREFIX = "error-window:";
    private static final Duration WINDOW_TIME = Duration.ofSeconds(60);
    private static final Duration KEY_TTL = Duration.ofSeconds(120);

    private final StringRedisTemplate stringRedisTemplate;

    public ErrorWindow(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long record(String service, int partition, long offset, long timestampMillis) {
        String key = KEY_PREFIX + service;
        ZSetOperations<String, String> zSet = stringRedisTemplate.opsForZSet();

        zSet.add(key, partition + ":" + offset, timestampMillis);
        stringRedisTemplate.expire(key, KEY_TTL);
        zSet.removeRangeByScore(key, 0, System.currentTimeMillis() - WINDOW_TIME.toMillis());
        return zSet.zCard(key);
    }
}
