package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.injector.methods.Insert;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 先判断当前的phone来进行判断
        // 1.如果不是11位 直接返回异常
        if(phone.length() != 11){
            return Result.fail("手机号码不是11位");
        }
        // 2.在判断当前这个手机号格式问题 如果有问题返回异常
        if(!phone.matches("1[0-9]{10}$")){
            return Result.fail("当前手机号格式不正确");
        }
        // 3.在使用random生成一个验证码 这个验证码打印到这个控制台
        String code = RandomUtil.randomNumbers(6);
        System.out.println("当前验证码为:" + code);
        // 4.使用redis进行存储
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,1,TimeUnit.MINUTES);
        // 5.在返回前端
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {

        // 先查寻当前的手机号和当前的格式
        // 如果我这里写了 != 11,如何有值进来了,可能会报错
        if(loginForm.getPhone() == null || !loginForm.getPhone().matches("1[0-9]{10}$")){
            return Result.fail("当前的手机号格式有问题");
        }

        // 在进行验证码的比对
        // 先查询redis里面的数据
        String code = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + loginForm.getPhone());

        // 在进行判断
        if(loginForm.getCode() == null || code == null || !code.equals(loginForm.getCode())){
            return Result.fail("当前短信验证码错误");
        }

        // 在根据当前的手机号进行查询相关的数据
        User user = query().eq("phone", loginForm.getPhone()).one();
        if(user == null){
            user = new User();
            user.setPhone(loginForm.getPhone());
            user.setNickName("user_" +RandomUtil.randomNumbers(10));
            save(user);
        }

        // 因为实体类的信息隐私信息太多 会产生一些安全问题 所以我们把一些无关紧要的字段放到当前的dto里面
        UserDTO userDTO = new UserDTO();
        BeanUtil.copyProperties(user,userDTO);

        String jsonStr = JSONUtil.toJsonStr(userDTO);

        String token = UUID.randomUUID().toString();

        stringRedisTemplate.opsForValue().set("login:user:" + token,jsonStr,30,TimeUnit.MINUTES);

        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        // 3.1格式排序
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.写入redis setbit key offset 1
        // 时间要减去1 因为当前的数据必须是从0开始算的
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth -1 ,true);
        // 使用redis进行签到
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        // 3.1格式排序
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.获取本月截止今天为止得到所有的签到记录,返回的是一个十进制的数字
        List<Long> result = stringRedisTemplate.opsForValue()
                .bitField(key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        // 本月一次都没签过,key 可能不存在,直接返回 0
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        // 拆箱前先判 null,同时 num 为 0 说明一位都没签到,不用进循环
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        // 6.循环遍历:从最低位(今天)开始往前数
        int count = 0;
        while (true) {
            // 7.让这个数字与1做与运算,得到数字最低的一个bit位
            // 8.判断这个bit位是否为0
            if ((num & 1) == 0) {
                // 9.为0说明这天没签到,连续签到中断
                break;
            }
            // 不为0说明已签到,计数器+1
            count++;
            // 10.把数字无符号右移一位,原来的第二位变成新的最低位(相当于看前一天)
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        // 1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        // 2.保存用户
        save(user);
        return user;
    }
}
