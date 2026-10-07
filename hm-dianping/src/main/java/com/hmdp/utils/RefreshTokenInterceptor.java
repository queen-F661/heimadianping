package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import jodd.util.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

public class RefreshTokenInterceptor implements HandlerInterceptor {
    /**
     * 因为这个类没有放到Ioc容器中进行
     * 所以他就不会自动的注入 所以我们只能使用构造函数的方式进行注入
     * */
    private StringRedisTemplate stringRedisTemplate;


    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 在controller执行之前执行
     * false 拦截
     * true 放行
     * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // TODO 1.获取token 从前端请求头中获取过来
        // HttpSession session = request.getSession();
        String token = request.getHeader("authorization");

        // 直接交给下一个拦截器进行判断
        if (StringUtil.isBlank(token)) {
            return true;
        }

        // 2.根据当前token来获取当前的redis value还有没有值
        // UserDTO user = (UserDTO) session.getAttribute("user");
        Map<Object, Object> user = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);

        // 判断当前entries有没有值
        // 3.判断当前用户是否存在
        // 这个也是 当前如果没有值也直接放行给redis
        if(user.isEmpty()){
            return true;
        }
        // 因为你要转换成当前的dto对象才能把数据存进去(进行转换(Map -> dto)) 对象
        // 第一个参数 是当前map 第二个是对象 第三个是当前错误要不要忽略
        UserDTO userDTO = BeanUtil.fillBeanWithMap(user, new UserDTO(), false);
        // 5.如果存在,就把当前数据存在线程域里面了
        UserHolder.saveUser(userDTO);
        // 6.还要刷新当前的时间 因为防止当前的用户就半个小时就消除了
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,3, TimeUnit.MINUTES);
        return true;
    }

    /**
     * 在controller执行之后执行
     * */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        /**
         * 等这个controller结束的时候就
         * */
        UserHolder.removeUser();

    }
}
