package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private final static String KEY_PREFIX = "lock:";

    private final StringRedisTemplate redisTemplate;
    private String name;

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }

    /**
     * 获取锁
     * */
    @Override
    public boolean tryLock(long timeoutSec) {
        long threadId = Thread.currentThread().getId();
        Boolean success = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId + "", timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        redisTemplate.delete(KEY_PREFIX + name);
    }

}
