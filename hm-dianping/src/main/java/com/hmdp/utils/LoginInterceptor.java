package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class LoginInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * @param request 请求对象
     * @param response 相应对象
     * @param handler 拿去相应的方法(发起请求的方法信息)
     * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        // 1.先获取到当前的token
        String token = request.getHeader("token");
        // 2.在根据当前的redis获取到当前的用户(使用token)
        String user = stringRedisTemplate.opsForValue().get("login:user" + token);
        // 2.判断当前的用户为不为空
        if(user == null){
            return false;
        }
        // 3.在就把值存储到线程对象里面
        UserDTO bean = JSONUtil.toBean(user, UserDTO.class);
        UserHolder.saveUser(bean);
        // 4.在进行刷新时间操作
        stringRedisTemplate.expire("login:user" + token,30, TimeUnit.MINUTES);
        // 5.在返回
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable ModelAndView modelAndView) throws Exception {
        UserHolder.removeUser();
    }
}
