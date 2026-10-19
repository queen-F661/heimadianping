package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.ssh.JschUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透解决方案
        // Shop shopUser = queryByIDPenetration(id);
        // 互斥锁实现缓存击穿问题
        // Shop shopUser  = queryByIDBreakdown01(id);
        // 基于逻辑过期解决缓存击穿
        Shop shopUser = queryByIDBreakdown02(id);
        // 6.在把相应的数据存入返回
        return Result.ok(shopUser);
    }

    // 缓存穿透
    public Shop queryByIDPenetration(Long id){
        // 1.先使用redis查询相关的数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.有值直接返回
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
            // return Result.ok(shop);
        }
        if (shopJson != null && shopJson.equals("")) {
            return null;
            // return Result.fail("当前商铺不存在");
        }

        // 3.没有值查询数据库
        Shop shopUser = getById(id);
        // 4.数据库没值直接返回
        if(shopUser == null){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
            // return Result.fail("当前商铺不存在");
        }

        // 5.有值的话把值先存入当前的redis
        String jsonStr = JSONUtil.toJsonStr(shopUser);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,jsonStr,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        // 6.在把相应的数据存入返回
        return shopUser;
    }

    /**
     * @param id 商铺的id
     * 基于互斥锁解决缓存击穿
     * */
    public Shop queryByIDBreakdown01(Long id){
        // 1.先使用redis查询相关的数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.有值直接返回
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
            // return Result.ok(shop);
        }
        if (shopJson != null && shopJson.equals("")) {
            return null;
            // return Result.fail("当前商铺不存在");
        }

        // 先获取锁
        String uuid = UUID.randomUUID().toString();
        Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + id, uuid, 10, TimeUnit.MINUTES);


        try {
            while(Boolean.TRUE.equals(lock) == false){
                Thread.sleep(50);
                lock = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + id,uuid, 10,TimeUnit.MINUTES);
            }

            String shoptest = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            if(StrUtil.isNotBlank(shoptest)){
                Shop shop = JSONUtil.toBean(shoptest, Shop.class);

                return shop;
            }
            if (shoptest != null && shoptest.equals("")) {
                return null;
                // return Result.fail("当前商铺不存在");
            }


            // 3.没有值查询数据库
            Shop shopUser = getById(id);
            // 4.数据库没值直接返回
            if(shopUser == null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
                // return Result.fail("当前商铺不存在");
            }

            // 5.有值的话把值先存入当前的redis
            String jsonStr = JSONUtil.toJsonStr(shopUser);
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,jsonStr,CACHE_SHOP_TTL,TimeUnit.MINUTES);
            // 6.在把相应的数据存入返回
            return shopUser;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            stringRedisTemplate.execute(
                    UNLOCK_SCRIPT,
                    Collections.singletonList(LOCK_SHOP_KEY + id),
                    uuid
            );
        }
    }


    /**
     * @param id 商铺的id
     * 基于逻辑过期解决缓存击穿
     * */
    // 创建线程
    private static final ExecutorService CACHE_REBUILD_EXECUTOR =
            Executors.newFixedThreadPool(10);
    public Shop queryByIDBreakdown02(Long id){
        // 1.先使用redis查询相关的数据
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // null 和空串都算没缓存（空串是以前方案写进去的空值缓存 / 旧格式脏数据）
        if (StrUtil.isBlank(shopJson)) {
            // 冷启动兜底：这个 key 从没预热过，查一次数据库并写入带逻辑过期的缓存
            // （逻辑过期方案官方假设热点 key 已预热，这里补上兜底，避免首次访问一直返回 null）
            Shop shopDb = getById(id);
            if (shopDb == null) {
                return null;
            }
            saveShop2Redis(id, shopDb, CACHE_SHOP_TTL * 60);
            return shopDb;
        }

        // 2.反序列化成 RedisData，注意 data 是 Object，Hutool 转出来是 JSONObject，不能直接强转成 Shop
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = null;
        if (redisData.getData() != null) {
            shop = BeanUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        }

        LocalDateTime expireTime = redisData.getExpireTime();
        LocalDateTime now = LocalDateTime.now();

        // 3.没过期（或者压根没有 expireTime，说明是旧格式数据）就直接返回
        // expireTime 为 null 也要防住，不然这里就是个 NPE 大坑
        if (expireTime != null && expireTime.isAfter(now)) {
            return shop;
        }

        // 4.已过期，尝试拿互斥锁，只有拿到锁的线程去重建缓存
        String uuid = UUID.randomUUID().toString();
        Boolean lock = stringRedisTemplate.opsForValue()
                .setIfAbsent(LOCK_SHOP_KEY + id, uuid, LOCK_SHOP_TTL, TimeUnit.SECONDS);
        if(Boolean.TRUE.equals(lock)){
            CACHE_REBUILD_EXECUTOR.submit(()->{
               try {
                   // 5.查询数据库
                   Shop shopUser = getById(id);
                   // 6.数据库没值：不再写空值缓存，否则下次读出来又被当 RedisData 解析，死循环 NPE
                   if(shopUser == null){
                       return null;
                   }

                   // 7.有值的话包装成 RedisData 写回 redis（逻辑过期：key 本身不设 TTL）
                   saveShop2Redis(id, shopUser, CACHE_SHOP_TTL * 60);
                   return shopUser;
               } finally {
                   unlock(LOCK_SHOP_KEY + id, uuid);
               }
            });
        }
        // 8.没拿到锁的线程直接返回旧数据（哪怕过期了，也比没有强）
        return shop;
    }

    /**
     * 店铺缓存预热：把店铺数据包装成 RedisData（带逻辑过期时间）写入 redis
     * 逻辑过期方案下 key 不设 TTL，靠 expireTime 字段判断过期
     * @param id 商铺 id
     * @param shop 商铺数据
     * @param expireSeconds 逻辑过期时间（秒）
     */
    public void saveShop2Redis(Long id, Shop shop, Long expireSeconds) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        redisData.setData(shop);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 释放锁：必须用 Lua 脚本保证「判断 + 删除」原子性，
     * 分两步写（get 再 del）在并发下可能把别人的锁删掉
     */
    private void unlock(String key, String uuid) {
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(key),
                uuid
        );
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }


    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT =
            new DefaultRedisScript<Long>(
                    "if redis.call('get', KEYS[1]) == ARGV[1] " +
                            "then return redis.call('del', KEYS[1]) " +
                            "else return 0 end",
                    Long.class
            );
}
