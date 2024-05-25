package com.day0303;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 17:46
 * Description:
 */
public abstract class BaseHelper {

    // 初始化加载配置文件
    public abstract Properties init(String path);

    // 输入
    public abstract void put(String src, String inPath);

    // 生产者（发送数据）
    public abstract void produce(String src, String key, byte[] data);

    // 消费者（接收数据）
    public abstract void consume(String src,String outPath);

    // 输出
    public abstract void get(String offset, byte[] value, String outPath);

    // 释放资源
    public abstract void destroy();

}
