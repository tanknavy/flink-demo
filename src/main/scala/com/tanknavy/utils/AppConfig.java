package com.tanknavy.utils;
import com.typesafe.config.Config;

/**
 * Author: Alex Cheng 11/29/2020 7:41 PM
 */

//参数配置类方法二，使用typesafe的Config从resources下面读取配置
public class AppConfig {

    private final String url;
    private final String user;
    private final String password;

    //调用时初始化ConfigFactory.load()返回Config，
    public AppConfig(Config config) { //Config是接口类型，要使用时new AppConfig(ConfigFactory.load())
        this.url = config.getString("jdbc.url");
        this.user = config.getString("dbuser");
        this.password = config.getString("password");
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
