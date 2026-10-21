package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private StringRedisTemplate stringRedisTemplate;

    private static final int COUNT_BITS = 32;

    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 全局ID生成器
     * 全局ID生成器,是一种在分布式系统下用来生成全局唯一ID的工具
     * 唯一性 高可用 高性能 递增性 安全性
     * ID的组成部分:
     * 符号位: 1bit,永远位0
     * 时间戳: 31bit,以秒位单位,可以使用69年
     * 序列号:32bit,秒内的计数器,支持每秒产生2^32个不同ID
     * */
    public long nextId(String keyPrefix) {
        // 先定义一个固定值 这个固定值是用来进行计算唯一时间戳
        LocalDateTime begin = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        LocalDateTime now = LocalDateTime.now();

        // 在把他们转换成long进行相减
        long beginLong = begin.toEpochSecond(ZoneOffset.UTC);
        long nowLong = now.toEpochSecond(ZoneOffset.UTC);

        // 计算当前的时间戳
        beginLong = nowLong - beginLong;

        // 在使用redis进行计算当前的序列号

        // 使用FORMATTER来规范当前时间转换的格式
        String date = now.format(FORMATTER);
        String key =  "icr:" + keyPrefix + ":" + date;
        // 使用api increment 生成序列号
        Long increment = stringRedisTemplate.opsForValue().increment(key);


        // 直接返回数据
        return  beginLong << COUNT_BITS | increment;
    }
}
