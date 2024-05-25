package com.utils;

import com.alibaba.fastjson.JSON;
import org.apache.avro.data.Json;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/16
 * Time: 17:14
 * Description:
 */
public class MRUtil {

    // 字符串数字转换为Integer类型
    public static Integer intTreat(String num){
        try {
            return Integer.parseInt(num);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    // 字符串数字转换为Long类型
    public static Long longTreat(String num){
        try {
            return Long.parseLong(num);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public static void main(String[] args) throws IOException {
        String ip = "113.136.12.70";
        System.out.println(getIPArea(ip));
    }

    // 获取IP的归属地
    // IP转换为JSON的API：https://ip.useragentinfo.com/json?ip=113.136.12.70
    public static String getIPArea(String ip) throws IOException {
        // 1、请求URL：每传入一次IP访问一次API
        String url = "https://ip.useragentinfo.com/json?ip="+ ip;
        // 2、获取Http客户端对象(浏览器对象)
        CloseableHttpClient client = HttpClients.createDefault();
        // 3、创建请求发送方式对象
        HttpGet get = new HttpGet(url);
        // 4、发送请求，获得响应对象
        CloseableHttpResponse response = client.execute(get);
        // 5、从响应对象中获得实体
        HttpEntity entity = response.getEntity();
        // 6、将实体转换为JSON格式的字符串
        // {"country": "中国", "short_name": "CN", "province": "陕西省", "city": "西安市", "area": "", "isp": "电信", "net": "", "ip": "113.136.12.70", "code": 200, "desc": "success"}
        String json = EntityUtils.toString(entity);
        // 7、Json格式的字符串转换为Map：使用Ali工具fastjson依赖
        Map<String,String> map = (Map)JSON.parse(json);
        // 8、输出结果
        String country = map.get("country");
        String province = map.get("province");
        String city = map.get("city");
        String res;
        if (province.equals(city)){
            res = country+province;
        }else {
            res = country+province+city;
        }
        return res;
    }

}
