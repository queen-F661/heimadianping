package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 缓存重建专用线程池，异步重建不阻塞主请求线程 */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR =
            Executors.newFixedThreadPool(10);

    /** 互斥锁默认过期时间（秒） */
    private static final long LOCK_TTL_SECONDS = 10L;

    /**
     * @param <R> 当前这个方法泛型类型
     * @param keyPrefix redis key的前缀
     * @param id 查询数据的类型
     * @param clazz 返回类型 转成这个类型返回给controller
     * @param dbFallback 查询数据的方法
     * */
    public <R,ID> R queryByIDPenetration(String keyPrefix,ID id,Class<R> clazz,Function<ID,R> dbFallback){
        // 1.先使用redis查询相关的数据
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        // 2.有值直接返回
        if(StrUtil.isNotBlank(shopJson)){
            R r = JSONUtil.toBean(shopJson, clazz);
            return r;
            // return Result.ok(shop);
        }
        if (shopJson != null && shopJson.equals("")) {
            return null;
            // return Result.fail("当前商铺不存在");
        }

        // 3.没有值查询数据库
        R shopUser = dbFallback.apply(id);
        // 4.数据库没值直接返回
        if(shopUser == null){
            stringRedisTemplate.opsForValue().set(keyPrefix + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
            // return Result.fail("当前商铺不存在");
        }

        // 5.有值的话把值先存入当前的redis
        String jsonStr = JSONUtil.toJsonStr(shopUser);
        stringRedisTemplate.opsForValue().set(keyPrefix + id,jsonStr,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        // 6.在把相应的数据存入返回
        return shopUser;
    }

    /**
     * @param <R> 当前这个方法泛型类型
     * @param keyPrefix redis key的前缀
     * @param id 查询数据的类型
     * @param clazz 返回类型 转成这个类型返回给controller
     * @param dbFallback 查询数据的方法
     * */
    public <R,ID> R queryByIDBreakdown01(String keyPrefix,String lockUser,ID id,Class<R> clazz,Function<ID,R> dbFallback){
        // 1.先使用redis查询相关的数据
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        // 2.有值直接返回
        if(StrUtil.isNotBlank(shopJson)){
            R r = JSONUtil.toBean(shopJson, clazz);
            return r;
        }
        if (shopJson != null && shopJson.isEmpty()) {
            return null;
        }

        // 先获取锁
        String uuid = UUID.randomUUID().toString();
        Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(lockUser + id, uuid, 10, TimeUnit.MINUTES);

        try {
            while(!Boolean.TRUE.equals(lock)){
                Thread.sleep(50);
                lock = stringRedisTemplate.opsForValue().setIfAbsent(lockUser + id,uuid, 10,TimeUnit.MINUTES);
            }

            String shoptest = stringRedisTemplate.opsForValue().get(keyPrefix + id);
            if(StrUtil.isNotBlank(shoptest)){
                R r = JSONUtil.toBean(shoptest, clazz);

                return r;
            }
            if (shoptest != null && shoptest.isEmpty()) {
                return null;
                // return Result.fail("当前商铺不存在");
            }


            // 3.没有值查询数据库
            R shopUser = dbFallback.apply(id);
            // 4.数据库没值直接返回
            if(shopUser == null){
                stringRedisTemplate.opsForValue().set(keyPrefix + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
                // return Result.fail("当前商铺不存在");
            }

            // 5.有值的话把值先存入当前的redis
            String jsonStr = JSONUtil.toJsonStr(shopUser);
            stringRedisTemplate.opsForValue().set(keyPrefix + id,jsonStr,CACHE_SHOP_TTL,TimeUnit.MINUTES);
            // 6.在把相应的数据存入返回
            return shopUser;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            stringRedisTemplate.execute(
                    UNLOCK_SCRIPT,
                    Collections.singletonList(lockUser + id),
                    uuid
            );
        }
    }

    /**
     * 【方案三】基于逻辑过期解决缓存击穿
     * <p>
     * 封装要点：
     * 1.原来写死的 Shop / getById 全部参数化：clazz 管类型，dbFallback 管查库
     * 2.data 是 Object，Hutool 转出来是 JSONObject，必须 BeanUtil 二次转换，不能强转
     * 3.expireTime 要判 null，旧格式数据没这个字段，不判就是 NPE
     * 4.重建丢到线程池异步做，主线程拿着旧数据立刻返回
     *
     * @param keyPrefix     缓存 key 前缀，如 CACHE_SHOP_KEY
     * @param lockUser      锁 key 前缀，如 LOCK_SHOP_KEY
     * @param id            业务 id
     * @param clazz         返回类型，如 Shop.class
     * @param dbFallback    查库回调，如 this::getById
     * @param expireSeconds 逻辑过期时间（秒）
     */
    public <R, ID> R queryByIDBreakdown02(String keyPrefix, String lockUser, ID id, Class<R> clazz,
                                          Function<ID, R> dbFallback, Long expireSeconds) {
        String key = keyPrefix + id;

        // 1.先查缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // null 和空串都算没缓存（空串是别的方案留下的空值缓存）
        if (StrUtil.isBlank(json)) {
            // 冷启动兜底：这个 key 没预热过，查一次库并写入逻辑过期缓存
            R r = dbFallback.apply(id);
            if (r == null) {
                return null;
            }
            saveShop2Redis(key, r, expireSeconds);
            return r;
        }

        // 2.反序列化成 RedisData
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R value = null;
        if (redisData.getData() != null) {
            value = BeanUtil.toBean((JSONObject) redisData.getData(), clazz);
        }

        // 3.没过期直接返回；expireTime 为 null 说明是旧格式脏数据，当作已过期去重建
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime != null && expireTime.isAfter(LocalDateTime.now())) {
            return value;
        }

        // 4.已过期，抢锁。抢到锁的线程去异步重建，其余线程直接返回旧数据
        String lockKey = lockUser + id;
        String uuid = UUID.randomUUID().toString();
        if (tryLock(lockKey, uuid)) {
            try {
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    try {
                        R r = dbFallback.apply(id);
                        // 库里没有就不写缓存，避免空值下次被当 RedisData 解析
                        if (r == null) {
                            return;
                        }
                        saveShop2Redis(key, r, expireSeconds);
                    } catch (Exception e) {
                        log.error("缓存重建失败, key = {}", key, e);
                    } finally {
                        unlock(lockKey, uuid);
                    }
                });
            } catch (Exception e) {
                // 线程池拒绝任务时，主线程得把锁放掉，不然要等 10 秒自动过期
                log.error("提交缓存重建任务失败, key = {}", key, e);
                unlock(lockKey, uuid);
            }
        }
        // 5.没抢到锁的线程直接返回旧数据（过期的数据也比没有强）
        return value;
    }

    /**
     * 写入带逻辑过期时间的缓存：key 本身不设 TTL，靠 value 里的 expireTime 判断
     *
     * @param key           完整 key
     * @param value         要缓存的对象（注意是对象，不是 Class）
     * @param expireSeconds 逻辑过期时间（秒）
     */
    public <R> void saveShop2Redis(String key, R value, Long expireSeconds) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /** 同上，按「时长 + 单位」传参的版本，方便测试类调用 */
    public <R> void setWithLogicalExpire(String key, R value, Long time, TimeUnit timeUnit) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /** 尝试获取互斥锁：SETNX + 过期时间一步到位，避免拿到锁的线程挂了造成死锁 */
    private boolean tryLock(String key, String uuid) {
        return Boolean.TRUE.equals(
                stringRedisTemplate.opsForValue()
                        .setIfAbsent(key, uuid, LOCK_TTL_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * 释放锁：必须用 Lua 脚本保证「判断 + 删除」原子性
     * 分两步写（get 再 del）在并发下可能把别人的锁删掉
     */
    private void unlock(String key, String uuid) {
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(key),
                uuid
        );
    }

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT =
            new DefaultRedisScript<Long>(
                    "if redis.call('get', KEYS[1]) == ARGV[1] " +
                            "then return redis.call('del', KEYS[1]) " +
                            "else return 0 end",
                    Long.class
            );
}
