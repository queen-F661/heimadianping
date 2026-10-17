package com.hmdp.utils;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {

    /**
     * @param request 请求对象
     * @param response 相应对象
     * @param handler 拿去相应的方法(发起请求的方法信息)
     * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        // 先获取当前的session对象(从前端)
        HttpSession session = request.getSession();
        // 在获取当前session中的用户
        Object user = session.getAttribute("user");
        // 在判断到当前的用户存不存在
        if(user == null){
            response.setStatus(401);
            // 拦截
            return false;
        }
        // 存在,保存用户信息到threadlocal里面
        UserHolder.saveUser((UserDTO)user);
        // 放行
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable ModelAndView modelAndView) throws Exception {
        UserHolder.removeUser();
    }
}
