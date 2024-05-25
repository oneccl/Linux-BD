package com.utils;

import com.baidu.aip.nlp.AipNlp;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/14
 * Time: 19:09
 * Description:
 */
public class BaiduAipNlp {

    //设置APPID/AK/SK
    private static final String APP_ID = "30414483";
    private static final String API_KEY = "yXG3ZGtMGrAZqlwiZvqj05Vf";
    private static final String SECRET_KEY = "M3pTTHMbGANPO0GG6ctKW6cswts6Z2i6";
    // 初始化一个AipNlp
    private static final AipNlp client = new AipNlp(APP_ID, API_KEY, SECRET_KEY);


    static {
        // 可选：设置网络连接参数
        client.setConnectionTimeoutInMillis(2000);
        client.setSocketTimeoutInMillis(60000);
    }

    public static void main(String[] args) {

        System.out.println(parse("任职要求：Hadoop/Hdfs，Flink, Hive，Spark，ETL，数据挖掘"));

    }

    public static List<String> parse(String text) {
        // 结果
        List<String> list = new LinkedList<>();
        // 调用接口
        try {
            Thread.sleep(500);
            JSONObject res = client.lexer(text, null);
            JSONArray jsonArr = res.getJSONArray("items");
            for (Object json : jsonArr) {
                JSONObject j = (JSONObject) json;
                String item = j.getString("item");
                // \u4e00-\u9fa5: 中文匹配 或 a-zA-Z: 英文匹配
                if (item.matches("[\\u4e00-\\u9fa5a-zA-Z]+")){
                    list.add(item);
                }
            }
        } catch (Exception e) {
            parse(text);
        }
        return list;
    }

}
