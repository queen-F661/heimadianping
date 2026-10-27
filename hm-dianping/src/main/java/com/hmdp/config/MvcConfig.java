package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 当前拦截器里面配置的类
     * */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoginInterceptor(stringRedisTemplate))
                .excludePathPatterns(
                        "/user/login",
                        "/user/code",
                        // 商铺相关:首页不登录也要能看
                        "/shop/**",
                        // 排雷:商铺"分类"是另一个路径,不是 /shop 的子路径!
                        //      /shop/** 只能匹配 /shop/xxx,匹配不到 /shop-type/list,
                        //      漏掉这行首页分类图标就全是空的(接口返回 401)
                        "/shop-type/**",
                        "/blog/hot",
                        "/voucher/**",
                        "shop-type/list"
                )
                .order(1)
            ;
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
                .addPathPatterns("/**")
                .order(0);
    }
}
