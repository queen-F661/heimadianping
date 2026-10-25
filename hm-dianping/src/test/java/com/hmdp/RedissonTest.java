package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
class RedissonTest {
    private RLock lock;

//    @Resource
//    private RedissonClient redissonClient01;
//
//    @Resource
//    private RedissonClient redissonClient02;
//
//    @Resource
//    private RedissonClient redissonClient03;
    @Resource
    private RedissonClient redissonClient;

    @BeforeEach
    void setUp() {
//        RLock lock01 = redissonClient01.getLock("order");
//        RLock lock02 = redissonClient02.getLock("order");
//        RLock lock03 = redissonClient03.getLock("order");
//
//        // 联合创锁 multiLock
//        redissonClient01.getMultiLock(lock01, lock02, lock03);
    }

    @Test
    void method1() throws InterruptedException {
        // 尝试获取锁
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock) {
            log.error("获取锁失败 .... 1");
            return;
        }
        try {
            log.info("获取锁成功 .... 1");
            method2();
            log.info("开始执行业务 ... 1");
        } finally {
            log.warn("准备释放锁 .... 1");
            lock.unlock();
        }
    }
    void method2() {
        // 尝试获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败 .... 2");
            return;
        }
        try {
            log.info("获取锁成功 .... 2");
            log.info("开始执行业务 ... 2");
        } finally {
            log.warn("准备释放锁 .... 2");
            lock.unlock();
        }
    }
}
