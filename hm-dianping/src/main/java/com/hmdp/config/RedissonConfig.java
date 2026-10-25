package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() throws  Exception{
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.45.128:6379")
                                .setPassword("123456");
        return Redisson.create(config);
    }

//    @Bean
//    public RedissonClient redissonClient02() throws  Exception{
//        // 配置
//        Config config = new Config();
//        config.useSingleServer().setAddress("redis://192.168.45.128:6379");
//        return Redisson.create(config);
//    }
//
//    @Bean
//    public RedissonClient redissonClient03() throws  Exception{
//        // 配置
//        Config config = new Config();
//        config.useSingleServer().setAddress("redis://192.168.45.128:6379");
//        return Redisson.create(config);
//    }
}
