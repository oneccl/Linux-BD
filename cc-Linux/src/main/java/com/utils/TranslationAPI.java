package com.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/28
 * Time: 9:09
 * Description:
 */
public class TranslationAPI {

    public static void main(String[] args) {
        System.out.println(translate("Stateful Computations over Data Streams"));
    }

    public static String translate(String doc){
        try {
            URL url = new URL("http://fanyi.youdao.com/translate?&doctype=json&type=AUTO&i="+ doc);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            // post请求
            con.setRequestMethod("POST");
            // 请求头
            con.setRequestProperty("Content-Type", "application/json");
            // 请求体
            String requestBody = "{\"doctype\":\"json\",\"type\":\"AUTO\",\"i\":\""+ doc +"\"}";
            con.setDoOutput(true);
            con.setDoInput(true);
            OutputStream os = con.getOutputStream();
            os.write(requestBody.getBytes());
            os.flush();
            os.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine = in.readLine();
            StringBuilder content = new StringBuilder();
            while (inputLine != null) {
                content.append(inputLine);
                inputLine = in.readLine();
            }
            in.close();
            String result = content.toString().trim();
            System.out.println(result);
            JSONObject jsonObj = JSON.parseObject(result);
            JSONArray jsonArray = jsonObj.getJSONArray("translateResult");
            JSONArray jsonArr = jsonArray.getJSONArray(0);
            JSONObject jsonObject = jsonArr.getJSONObject(0);
            return jsonObject.getString("tgt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
