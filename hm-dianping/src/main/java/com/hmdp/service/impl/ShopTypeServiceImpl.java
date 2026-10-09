package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
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

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> selectDall() {
        String ten = "cache:shopType:list";
        // 1.首先 你要查看当前redis有没有值
        // 因为你查这个数据不是总的数据,他是分几个 所以使用List进行
        String shopType = stringRedisTemplate.opsForValue().get(ten);

        // 这个是查询不为空的
        if (StrUtil.isNotBlank(shopType)) {
            // 2.有值就直接返回
            // 这个api是根据当前的json格式的值来进行查询
            List<ShopType> types = JSONUtil.toList(shopType, ShopType.class);
            return types;
        }
        // 3.没有值数据库进行查询
        List<ShopType> sort = query().orderByAsc("sort").list();
        // 4.没有值 直接返回空
        if(sort.isEmpty()){
            return null;
        }
        // 因为 这个redis要把值转换成String 通过
        // 5.有值 直接把数据存在当前的redis
        stringRedisTemplate.opsForValue().set(ten,JSONUtil.toJsonStr(sort));
        // 6.在把值进行返回
        return sort;
    }
}
