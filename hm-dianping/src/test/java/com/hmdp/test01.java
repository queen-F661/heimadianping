package com.hmdp;
import com.hmdp.entity.Shop;
import com.hmdp.utils.CacheClient;
import org.junit.jupiter.api.Test;
import com.hmdp.service.impl.ShopServiceImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
public class test01 {

    @Autowired
    private ShopServiceImpl shopService;

    @Autowired
    private CacheClient cacheClient;

    @Test
    public void test02() {

        shopService.saveShopRedis(1L,3L);
    }
}
