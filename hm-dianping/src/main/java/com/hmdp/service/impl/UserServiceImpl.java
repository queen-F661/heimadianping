package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.PageUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.injector.methods.Insert;
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
import org.springframework.beans.BeanUtils;
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
        // 1.判断当前手机号的格式
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不成功,就直接返回一个报错提示 "当前验证码的格式错误 等等"
            return Result.fail("当前输入的手机号的格式错误");
        }
        // 3.如果成功了 就调用工具包随机生成验证码(6位验证码)
        String code = RandomUtil.randomNumbers(6);
        // 4.在把数据存到session里面
        session.setAttribute("code",code);
        // 5.发送验证码
        log.info("发送短信验证码成功,验证码:{}",code);
        // 6.返回给前端
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.1先进行手机验证正不正确
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不成功,就直接返回一个报错提示 "当前验证码的格式错误 等等"
            return Result.fail("当前输入的手机号的格式错误");
        }
        // 1.2验证码的验证
        // 获取当session里面的验证码数据
        Object code = session.getAttribute("code");
        String loginFormCode = loginForm.getCode();
        // 在把当前的数据进行判断为不为空 如果为空,就直接返回当前的验证码为空的数据
        if(code == null || code.toString().equals(loginFormCode)){
            // 2.如果不成功,就直接返回一个报错提示 "当前验证码的格式错误 等等"
            Result.fail("当前验证码错误");
        }

        // 3.通过了,在在根据手机号进行查询数据 select * from tb_user where phone = ?
        // 这个是根据phone来查询相关的数据
        User user = query().eq("phone", phone).one();

        // 4.如果不存在,那么就创建新用户,把用户保存在数据库中
        if(user == null){
            user = createUserWith(phone);
        }

        // 5.如果存在(不存在),都要把他保存到session里面
        UserDTO userDTO = new UserDTO();
        BeanUtils.copyProperties(user,userDTO);
        session.setAttribute("user",userDTO);
        return Result.ok();
    }

    private User createUserWith(String phone) {
        // 当前的实体类
        User user = new User();
        // 插入数据
        user.setPhone(phone);
        user.setNickName("user_" + RandomUtil.randomNumbers(10));

        // 保存到数据库
        save(user);
        return user;
    }


}
