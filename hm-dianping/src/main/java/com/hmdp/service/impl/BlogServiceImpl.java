package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->  {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("当前博客不存在");
        }
        // 填充笔记作者的头像和昵称（复用下面已封装好的 queryBlogUser）
        queryBlogUser(blog);
        // 判断当前登录用户是否点过赞 isBlogLiked(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            // 用户未登录,无需查询是否点赞
            return;
        }
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        // 1.先判断当前当前的redis里面有没有值(当前的key是用做当前的用户id来进行给的)
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        // 1.先判断当前当前的redis里面有没有值(当前的key是用做当前的用户id来进行给的)
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // 2.如果没有值说明没有点赞
        // 这个里面如果没有值,就进入到当前这个逻辑里面
        if(score == null){
            // 2.1 先进行数据库的添加+1
            update().setSql("liked = liked + 1").eq("id",id).update();
            // 2.2 在往当前的redis的set集合 zadd key value score
            stringRedisTemplate.opsForZSet().add(key, userId.toString(),System.currentTimeMillis());
        }
        // 3.有值的情况下就是说明有点赞的
        else {
            // 3.1 先进行数据库的添加-1
            update().setSql("liked = liked - 1").eq("id",id).update();
            // 3.2 移除redis里面的数据
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 先查询当前在redis当前商铺查询前五的数据
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 5);
        if(top5 == null || top5.isEmpty()){
            return Result.ok();
        }
        // 如果当前为空就直接返回
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        // 在拿取到当前用户的id
        userService.listByIds(ids)
                .stream().map(user -> BeanUtil.copyProperties(user,UserDTO.class)).collect(Collectors.toList());
        ;
        // 根据当前用户的id来进行查询到当前的用户数据

        // 在把当前用户的数据排序进行返回

        return Result.ok();
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 0.参数校验:tb_blog 里 shop_id/title/images/content 都是 NOT NULL 且无默认值,
        //   缺任何一个都会在插入时抛 SQL 异常,这里提前拦住,给前端可读的提示
        if (blog.getShopId() == null) {
            return Result.fail("缺少店铺id，请从店铺详情页发布探店笔记");
        }
        if (StrUtil.isBlank(blog.getTitle())) {
            return Result.fail("笔记标题不能为空");
        }
        if (StrUtil.isBlank(blog.getImages())) {
            return Result.fail("请至少上传一张探店照片");
        }
        if (StrUtil.isBlank(blog.getContent())) {
            return Result.fail("笔记内容不能为空");
        }
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }
        blog.setUserId(user.getId());
        // 2.保存探店笔记
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败!");
        }

        // 推送笔记id给所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        for (Follow follow : follows) {
            // 1.获取粉丝id
            Long userId = follow.getUserId();
            // 推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }

    /**
     * @param max 上一次最小值
     * @param offset 偏移量
     * */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {

        // 1.先查询当前用户的id
        Long userId = UserHolder.getUser().getId();
        // 2.根据用户id来进行查询笔记id
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3.便利循环出当前的时间戳和笔记id
        // 3.1使用当前的list存储当前的笔记id
        ArrayList<Long> noteId = new ArrayList<>(typedTuples.size());
        // 3.2 因为当前需要求出最小时间 所以我们要把值拿出来才行,遍历覆盖掉成最后一个值自然的就是最小值
        long minTime = 0;
        // 3.3 偏移度
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 笔记id
            Long value = Long.valueOf(typedTuple.getValue());
            noteId.add(value);
            // 时间戳
            long time = typedTuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else {
                minTime = time;
                os = 1;
            }
        }
        // 在根据笔记id查询blog
        String idStr = StrUtil.join(",",noteId);
        List<Blog> blogs = query().in("id", noteId).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        }
        
        // 组装数据
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);

        return Result.ok(r);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
