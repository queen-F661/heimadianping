package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> list() {
        // 1.先进行查询相关的数据
        String sky = stringRedisTemplate.opsForValue().get("shop:list");

        // 2.如果有值就直接返回
        if(StrUtil.isNotBlank(sky)){
            return JSONUtil.toList(sky, ShopType.class);
        }
        // 3.没有值就直接查询相关的数据库信息
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        // 4.数据库如果没有值直接返回异常
        if(shopTypes == null){
            throw new RuntimeException("当前服务器异常");
        }
        // 5.把值传递给redis里面
        stringRedisTemplate.opsForValue().set("shop:list",JSONUtil.toJsonStr(shopTypes));
        // 6.数据库有值就直接返回数据库
        return shopTypes;
    }
}
