package funcs;

import org.apache.hadoop.hive.ql.exec.UDF;
import utils.IPUtil;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/21
 * Time: 18:51
 * Description:
 */

public class UdfIPToArea extends UDF {

    public static void main(String[] args) {
        System.out.println(evaluate("113.136.12.70"));
    }

    // 获取IP归属地
    public static String evaluate(String ip){
        try {
            return IPUtil.getIPArea(ip);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
