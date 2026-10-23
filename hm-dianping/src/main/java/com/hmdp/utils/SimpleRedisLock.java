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

    private final long threadId = Thread.currentThread().getId();

    private final String uuid = UUID.randomUUID().toString().replace("-", "");


    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }

    /**
     * 获取锁
     * */
    @Override
    public boolean tryLock(long timeoutSec) {
        Boolean success = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name,uuid + threadId , timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        Long execute = redisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX + name),
                uuid + threadId
        );
    }
//    public void unlock() {
//        // 先查询当前的锁 看是不是当前的业务
//        String Thread = redisTemplate.opsForValue().get(KEY_PREFIX + name);
//        try {
//            if (Thread != null && Thread.equals(uuid + threadId)) {
//                redisTemplate.delete(KEY_PREFIX + name);
//            }
//        } catch (Exception e) {
//            System.out.println("当前锁删除错误");
//        }
//    }
}
