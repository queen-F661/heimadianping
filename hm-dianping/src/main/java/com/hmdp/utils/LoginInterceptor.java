package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.beans.BeanUtils;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {

    /**
     * 在controller执行之前执行
     * false 拦截
     * true 放行
     * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.获取session
        HttpSession session = request.getSession();
        // 2.获取我前面登录的时候保存到当前用户里面的数据
        UserDTO user = (UserDTO) session.getAttribute("user");
        // 3.判断当前用户是否存在
        if(user == null){
            // 4.如果不存在,就直接拦截
            return false;
        }
        // 5.如果存在,就把当前数据存在线程域里面了
        UserHolder.saveUser(user);

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
