package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    // 时间戳(1970年到现在的时间秒数) 就是相当于是1970年到2026年
    public static final long BEGIN_TIMESTAMP = 1790816460L;
    public static final int COUNT_BITS = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 生成一个id值,因为要生成唯一的id值,并且要保证他的安全性
     * @param ketPrefix 业务前缀,防止当前的id值起冲突,保证他的唯一性
     * */
    public Long nextId(String ketPrefix){
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        // 用当前的现在的时间戳 - 开始的时间戳 得出还有多少年限
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        // 2.生成序列号
        // 获取当前日期.精确到天
        String format = now.format(DateTimeFormatter.ofPattern("yyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + ketPrefix + ":" + format);
        // 3.拼接并返回
        // 这个意识是把当前的数据往前移32位 在给count填位置,使用|这个来进行来填充
        return timestamp << COUNT_BITS | count;
    }


}
