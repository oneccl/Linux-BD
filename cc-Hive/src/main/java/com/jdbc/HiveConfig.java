package com.jdbc;

import java.lang.annotation.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/10/8
 * Time: 21:37
 * Description:
 */

@Target(ElementType.TYPE)  // 作用在类上
@Retention(RetentionPolicy.RUNTIME)  // 作用在运行时
@Inherited // 允许继承
public @interface HiveConfig {

    // 数据库驱动
    String driver() default "org.apache.hive.jdbc.HiveDriver";
    // 连接路径url
    String url() default "jdbc:hive2://bd91:10000/default";
    // 用户名
    String user() default "root";
    // 密码
    String pw() default "123456";

}
