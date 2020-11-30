package com.tanknavy.utils;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author: Alex Cheng 11/29/2020 7:32 PM
 */

//参数配置类方法三，使用ClassLoader拿到输入流，使用Properties使用流从resources下面读取配置
public class ConfigurationManager {

    private static Properties prop = new Properties();

    static {
        //从当前类获得ClassLoader，再拿到输入流
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("application.conf");
        try {
            prop.load(in); //从输入的byte stream中读取属性列表
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){ //读取并返回String类型属性，如果其他属性，再创建类似方法
        return prop.getProperty(key);
    }
}
