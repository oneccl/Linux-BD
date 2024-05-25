package utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/21
 * Time: 18:41
 * Description:
 */
public class IPUtil {

    public static void main(String[] args) throws IOException {
        String ip = "113.136.12.70";
        System.out.println(getIPArea(ip));
    }

    // 获取IP归属地
    public static String getIPArea(String ip) throws IOException{
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
        String[] arr = json.split(",");
        // 8、输出结果
        String country = arr[0].split(":")[1];
        String province = arr[2].split(":")[1];
        String city = arr[3].split(":")[1];
        String res;
        if (province.equals(city)){
            res = country+province;
        }else {
            res = country+province+city;
        }
        String[] resArr = res.split("\"");
        return resArr[1]+resArr[3]+resArr[5];
    }

}
