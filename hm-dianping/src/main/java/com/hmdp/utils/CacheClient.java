package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 解决redis缓存穿透的问题
     * @param key 当前redis的key值
     * @param value 当前的redis的value值
     * @param time 当前时间的参数
     * @param unit 时间单位
     * */
    public void set(String key,Object value,Long time,TimeUnit unit){
        // JSONUtil.toJsonStr 转换成一个String类型
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     *  解决redis缓存击穿问题
     *  @param key 当前redis的key值
     *  @param value 当前的redis的value值
     *  @param time 当前时间的参数
     *  @param unit 时间单位
     * */
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        // 用unit把时间换算成秒，避免"3分钟"被当成"3秒"用
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        // 写入redis里面
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透方案
     * @param
     * */
    public <R,ID> R queryWithPassThrough(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix + id;

        // 1.查到redis里面有没有值 这个会变成一个json
        String json = stringRedisTemplate.opsForValue().get(key);
        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
        if (BeanUtil.isNotEmpty(json)) {
            return JSONUtil.toBean(json,type);
        }

        // 因为上面那个api 会把当前的空字符串过滤掉,他会把除了有效的数据排除在外面 就比如说是“   ”这个也是false 只有是无效数据都会false
        // 通过判断不为空,就能判断当前的这个值为不为空字符串
        if(json != null){
            // return Result.fail("当前商铺没有数据");
            return null;
        }
        // 3.如果没有值 就进行数据的查找
        R r = dbFallback.apply(id);
        // 4.数据库的查找如果没有值,那么就直接返回404(或者说是前端约定好的方式)
        if(r == null){
            stringRedisTemplate.opsForValue().set(key,"",time,unit);
            return null;
        }
        // 6.存在,写入redis(时间用调用方传进来的time和unit，别自己写死)
        this.set(key,r,time,unit);

        return r;
    }

    /**
     * redis 缓存击穿
     * */
    /**
     * redis 缓存击穿解决方案
     * */
    // 开启一个线程池
    private final static ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithPassMutex(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.查询当前redis有没有这个对应的value
        String json = stringRedisTemplate.opsForValue().get(key);
        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
        if (BeanUtil.isEmpty(json)) {
            return null;
        }
        // 1.命中了 要进行判断当前的缓存过没过期(要进行把数据反序列化成一个对象)
        RedisData bean = JSONUtil.toBean(json, RedisData.class);
        // 这个是获取数据
        R r= JSONUtil.toBean((JSONObject) bean.getData(), type);
        // 这个是获取当前过期时间
        LocalDateTime expireTime = bean.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            // 2.未过期 那就直接返回
            return r;
        }

        // 3.过期了 那就要拿去互斥锁,在判断当前拿没拿到
        // 先设置当前的key
        // 这个互斥锁要根据业务来进行创建新的 要是都用一个 那么整个项目的请求都在等他一下
        String lockkey = LOCK_SHOP_KEY + id;
        boolean lock = tryLock(lockkey);
        // 4.没拿到,返回旧数据
        // 5.拿到了,也返回当前的旧数据,在开启一条线程,通过这个线程来进行数据库的访问和更新
        if(lock){
            // 二次检查：抢到锁后重新查一遍缓存
            // 因为拿到的锁可能是别人刚释放的，人家可能刚重建完，那就不用再查库了
            String jsonAgain = stringRedisTemplate.opsForValue().get(key);
            if (jsonAgain != null) {
                RedisData beanAgain = JSONUtil.toBean(jsonAgain, RedisData.class);
                if (beanAgain.getExpireTime().isAfter(LocalDateTime.now())) {
                    // 别人已经重建完了：还锁，直接返回最新数据
                    unlock(lockkey);
                    return JSONUtil.toBean((JSONObject) beanAgain.getData(), type);
                }
            }
            try {
                // TODO 开启新线程,并且返回当前的数据
                CACHE_REBUILD_EXECUTOR.submit(()->{
                    try {
                        // 查询数据库
                        R apply = dbFallback.apply(id);
                        // 在把数据放到当前的redis里面
                        this.setWithLogicalExpire(key,apply,time,unit);
                    } catch (Exception e) {
                        // 异步任务里抛异常没人接，必须记日志，不然重建失败了你都不知道
                        log.error("缓存重建失败, key:{}", key, e);
                    } finally {
                        // 不管干成干砸，都释放锁
                        unlock(lockkey);
                    }
                });
            } catch (Exception e) {
                // submit本身失败：任务没递出去，lambda里的unlock永远不会跑，必须在这还锁
                unlock(lockkey);
                throw new RuntimeException(e);
            }
        }
        // // 4 拿没拿到,都会返回旧数据
        return r;
    }
    // 获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 3, TimeUnit.MINUTES);
        // 如果说是当前为null或者说是false 他就返回一个false
        // 相反 这个里面为true的时候也可以进行转换的 这个里面最重要的一点,他帮我们转换成true
        return BooleanUtil.isTrue(flag);
    }
    // 释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
