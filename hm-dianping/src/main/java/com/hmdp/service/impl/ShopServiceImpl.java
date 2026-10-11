package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
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
import java.util.*;
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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {

        Shop shop = queryWithPassMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }

        return Result.ok(shop);
    }

    /**
     * redis 缓存击穿解决方案
     * */
    public Shop queryWithPassMutex(Long id) {

        String key = "cach:shop";
        // 1.查询当前redis有没有这个对应的value
        String shop = stringRedisTemplate.opsForValue().get(key + id);
        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
        if (BeanUtil.isNotEmpty(shop)) {
//            // 转成shop这个对象返回给前端
//            Shop shop1 = JSONUtil.toBean(shop, Shop.class);
//            // 2.如果有值就直接返回
//            return Result.ok(shop1);
            return JSONUtil.toBean(shop,Shop.class);
        }

        // 因为上面那个api 会把当前的空字符串过滤掉,他会把除了有效的数据排除在外面 就比如说是“   ”这个也是false 只有是无效数据都会false
        // 通过判断不为空,就能判断当前的这个值为不为空字符串
        if(shop != null){
            // return Result.fail("当前商铺没有数据");
            return null;
        }
        String lockkey = null;
        Shop shop1 = null;

        try {
            // 4实现缓存重建
            // 4.1 获取互斥锁  这个是缓存的key,不是说是前面传递过来的商品key(所以我们需要根据当前的key + id的方式在写一个) 每一个店铺有独一无二当前的key
            lockkey = "lock:shop:" + id;
            boolean isLock = tryLock(lockkey);
            // 4.2 一种是这个锁被别人拿去了,拿不到这个锁
            if(!isLock){
                Thread.sleep(50);
                // 等这个锁能拿到值了 不走这个循环了,这个递归就可以退出来了
                return queryWithPassMutex(id);
            }
            shop = stringRedisTemplate.opsForValue().get(key + id);
            if (BeanUtil.isNotEmpty(shop)) {
                return JSONUtil.toBean(shop,Shop.class);
            }
            // 4.3 一种是拿到当前的互斥锁,就进行查询数据库,在更新一下redis里面的值
            // 3.如果没有值 就进行数据的查找
            shop1 = getById(id);

            Thread.sleep(100);

            // 4.数据库的查找如果没有值,那么就直接返回404(或者说是前端约定好的方式)
            if(shop1 == null){
                // 为了解决当前 redis的缓存穿透的问题
                // 他在往redis数据存储的时候 会把当前的空值转成一个空字符串
    //            stringRedisTemplate.opsForValue().set(key + id,"",2,TimeUnit.MINUTES);
    //            return Result.fail("当前商铺没有数据");
                return shop1;
            }

            // 5.数据库有值 先把数据存储到redis
            // 在存之前 要把当前的数据转成json格式的 因为我们引进来的stringRedisTemplate必须是string类型的
            shop = JSONUtil.toJsonStr(shop1);
            stringRedisTemplate.opsForValue().set(key + id,shop);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.把当前的锁进行释放
            unlock(lockkey);
        }


        // 7.在把值相应到前端
        return shop1;
    }

    /**
     * redis 缓存穿透解决方案
     * */
    public Shop queryWithPassThrough(Long id){

        String key = "cach:shop";
        // 1.查询当前redis有没有这个对应的value
        String shop = stringRedisTemplate.opsForValue().get(key + id);
        // 这个api 是hutool里面提供的api,用来进行判断当前的值为不为空
        if (BeanUtil.isNotEmpty(shop)) {
//            // 转成shop这个对象返回给前端
//            Shop shop1 = JSONUtil.toBean(shop, Shop.class);
//            // 2.如果有值就直接返回
//            return Result.ok(shop1);
            return JSONUtil.toBean(shop,Shop.class);
        }

        // 因为上面那个api 会把当前的空字符串过滤掉,他会把除了有效的数据排除在外面 就比如说是“   ”这个也是false 只有是无效数据都会false
        // 通过判断不为空,就能判断当前的这个值为不为空字符串
        if(shop != null){
            // return Result.fail("当前商铺没有数据");
            return null;
        }
        // 3.如果没有值 就进行数据的查找
        Shop shop1 = getById(id);
        // 4.数据库的查找如果没有值,那么就直接返回404(或者说是前端约定好的方式)
        if(shop1 == null){
            // 为了解决当前 redis的缓存穿透的问题
            // 他在往redis数据存储的时候 会把当前的空值转成一个空字符串
//            stringRedisTemplate.opsForValue().set(key + id,"",2,TimeUnit.MINUTES);
//            return Result.fail("当前商铺没有数据");
            return shop1;
        }

        // 5.数据库有值 先把数据存储到redis
        // 在存之前 要把当前的数据转成json格式的 因为我们引进来的stringRedisTemplate必须是string类型的
        shop = JSONUtil.toJsonStr(shop1);
        stringRedisTemplate.opsForValue().set(key + id,shop);
        // 6.在把值相应到前端

        return shop1;
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


    @Override
    @Transactional
    public Result update(Shop shop) {
        String key = "cach:shop";
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
        stringRedisTemplate.delete(key + shop.getId());
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
