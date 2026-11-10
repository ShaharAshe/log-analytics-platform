package com.logpipe.processor.service;

import com.logpipe.processor.config.DetectionProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

@Service
public class ErrorWindow {

    private static final String KEY_PREFIX = "error-window:";
    private static final int TTL_WINDOW_MULTIPLE = 2;

    private final StringRedisTemplate stringRedisTemplate;
    private final Duration window;
    private final Duration keyTtl;

    public ErrorWindow(StringRedisTemplate stringRedisTemplate, DetectionProperties properties) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.window = properties.window();
        this.keyTtl = properties.window().multipliedBy(TTL_WINDOW_MULTIPLE);
    }

    public long record(String service, int partition, long offset, long timestampMillis) {
        String key = KEY_PREFIX + service;
        ZSetOperations<String, String> zSet = stringRedisTemplate.opsForZSet();

        zSet.add(key, partition + ":" + offset, timestampMillis);
        stringRedisTemplate.expire(key, keyTtl);
        zSet.removeRangeByScore(key, 0, System.currentTimeMillis() - window.toMillis());
        return zSet.zCard(key);
    }

    public long countRecent(String service) {
        long from = System.currentTimeMillis() - window.toMillis();
        Long count = stringRedisTemplate.opsForZSet().count(KEY_PREFIX + service, from, Double.POSITIVE_INFINITY);
        return Objects.isNull(count) ? 0 : count;
    }
}
