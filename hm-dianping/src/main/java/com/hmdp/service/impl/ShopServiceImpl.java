package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.io.ResolverUtil;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 解决缓存击穿
        // Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,id2 ->getById(id2),3L,TimeUnit.MINUTES);

//        互斥解决缓解击穿
//        Shop shop = queryWithPassMutex(id);

        // 逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithPassMutex(CACHE_SHOP_KEY,id,Shop.class,id2 ->getById(id2),3L,TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");
        }

        return Result.ok(shop);


    }

//    /**
//     * redis 缓存击穿解决方案
//     * */
//    // 开启一个线程池
//    private final static ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//    public Shop queryWithPassMutex(Long id) {
//        String key = "cach:shop";
//        // 1.查询当前redis有没有这个对应的value
//        String shop = stringRedisTemplate.opsForValue().get(key + id);
//        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
//        if (BeanUtil.isEmpty(shop)) {
//            return null;
//        }
//        // 1.命中了 要进行判断当前的缓存过没过期(要进行把数据反序列化成一个对象)
//        RedisData bean = JSONUtil.toBean(shop, RedisData.class);
//        // 这个是获取数据
//        Shop shop1 = JSONUtil.toBean((JSONObject) bean.getData(), Shop.class);
//        // 这个是获取当前过期时间
//        LocalDateTime expireTime = bean.getExpireTime();
//        if(expireTime.isAfter(LocalDateTime.now())){
//            // 2.未过期 那就直接返回
//            return shop1;
//        }
//
//        // 3.过期了 那就要拿去互斥锁,在判断当前拿没拿到
//        // 先设置当前的key
//        String lockkey = "lock:key:" + id;
//        boolean lock = tryLock(lockkey);
//        // 4.没拿到,返回旧数据
//        // 5.拿到了,也返回当前的旧数据,在开启一条线程,通过这个线程来进行数据库的访问和更新
//        if(lock){
//            try {
//                // TODO 开启新线程,并且返回当前的数据
//                CACHE_REBUILD_EXECUTOR.submit(()->{
//                   // 重建缓存
//                    this.saveShopRedis(id,30L);
//                });
//
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            } finally {
//                // 6.在释放当前redis里面的锁
//                unlock(lockkey);
//            }
//        }
//        // // 4 拿没拿到,都会返回旧数据
//        return shop1;
//    }
//
//    /**
//     * redis 缓存穿透解决方案
//     * */
//    public Shop queryWithPassThrough(Long id){
//
//        String key = "cach:shop";
//        // 1.查询当前redis有没有这个对应的value
//        String shop = stringRedisTemplate.opsForValue().get(key + id);
//        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
//        if (BeanUtil.isNotEmpty(shop)) {
////            // 转成shop这个对象返回给前端
////            Shop shop1 = JSONUtil.toBean(shop, Shop.class);
////            // 2.如果有值就直接返回
////            return Result.ok(shop1);
//            return JSONUtil.toBean(shop,Shop.class);
//        }
//
//        // 因为上面那个api 会把当前的空字符串过滤掉,他会把除了有效的数据排除在外面 就比如说是“   ”这个也是false 只有是无效数据都会false
//        // 通过判断不为空,就能判断当前的这个值为不为空字符串
//        if(shop != null){
//            // return Result.fail("当前商铺没有数据");
//            return null;
//        }
//        // 3.如果没有值 就进行数据的查找
//        Shop shop1 = getById(id);
//        // 4.数据库的查找如果没有值,那么就直接返回404(或者说是前端约定好的方式)
//        if(shop1 == null){
//            // 为了解决当前 redis的缓存穿透的问题
//            // 他在往redis数据存储的时候 会把当前的空值转成一个空字符串
////            stringRedisTemplate.opsForValue().set(key + id,"",2,TimeUnit.MINUTES);
////            return Result.fail("当前商铺没有数据");
//            return shop1;
//        }
//
//        // 5.数据库有值 先把数据存储到redis
//        // 在存之前 要把当前的数据转成json格式的 因为我们引进来的stringRedisTemplate必须是string类型的
//        shop = JSONUtil.toJsonStr(shop1);
//        stringRedisTemplate.opsForValue().set(key + id,shop);
//        // 6.在把值相应到前端
//
//        return shop1;
//    }


    // 提前把数据写入当前的redis(预热)
    public void saveShopRedis(Long id,Long expirSecond){
        // 先去数据查询数据(根据id来进行查询)
        Shop shop = getById(id);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if(shop == null){
            log.info("当前redis数据预热{}",shop);
        }
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        // 封装当前的redis时间
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expirSecond));
        // 在把数据返回给redis

        stringRedisTemplate.opsForValue().set("cache:shop" + id,JSONUtil.toJsonStr(redisData));
    }

//    // 获取锁
//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 3, TimeUnit.MINUTES);
//        // 如果说是当前为null或者说是false 他就返回一个false
//        // 相反 这个里面为true的时候也可以进行转换的 这个里面最重要的一点,他帮我们转换成true
//        return BooleanUtil.isTrue(flag);
//    }
//    // 释放锁
//    private void unlock(String key){
//        stringRedisTemplate.delete(key);
//    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        // 删除缓存必须删和查询同一个key，统一用常量拼，别手写字符串
        if(shop.getId() == null){
            return Result.fail("当前更新的值没得");
        }
       // 1.先更新数据库的数据
        boolean b = updateById(shop);
        // 判断当前的店铺存不存在
        if(!b){
           return Result.fail("当前店铺没有");
        }
        // 2.删除redis中的数据
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        // 返回当前的数据
        return Result.ok(shop);
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
}
