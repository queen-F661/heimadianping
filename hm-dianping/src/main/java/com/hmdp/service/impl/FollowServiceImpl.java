package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 1.判断当前的用户是关注还是不关注
        if(isFollow){
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
                if(isSuccess){
                    // 把关注用户的额id,放入redis的set集合 sadd userId followerUserId
                    stringRedisTemplate.opsForSet().add(key,followUserId.toString());
                }
            }else {
                log.info("取关删除");
            boolean isSucess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));
            if(isSucess){
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }}
        // 3.判断
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        // 1.查询是否关注
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {

        // 1.获取到当前的用户id
        Long id1 = UserHolder.getUser().getId();
        // 拼接当前的key
        String key01 = "follows:" + id;
        String key02 = "follows:" + id1;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key01, key02);
        if(intersect == null || intersect.isEmpty()){
            // 无交集
            return Result.ok();
        }
        // 解析id集合 把这个转成list集合 - 里面都是long类型
        List<Long> collect = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 查询用户
        List<UserDTO> collect1 = userService.listByIds(collect)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        ;
        return Result.ok(collect1);
    }
}
