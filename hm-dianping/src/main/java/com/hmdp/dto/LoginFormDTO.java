package com.hmdp.dto;

import lombok.Data;

@Data
public class LoginFormDTO {
    /**
     * 电话
     * */
    private String phone;
    /**
     * 验证码
     * */
    private String code;
    /**
     * 密码
     * */
    private String password;
}
