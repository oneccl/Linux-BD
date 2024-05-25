package com.jdbc;
import com.jdbc.HiveUtils;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/10/8
 * Time: 21:49
 * Description:
 */
@HiveConfig()
public class HiveUtilsTest {

    public static void main(String[] args) {
        HiveUtilsTest hut = new HiveUtilsTest();
        hut.hiveConfigTest();
        hut.hiveQueryTest();
    }

    Connection cn;

    // Hive连接测试
    public void hiveConfigTest(){
        cn = HiveUtils.connect(HiveUtilsTest.class);
        System.out.println(cn);
    }

    // 执行查询
    public void hiveQueryTest(){
        String hql = "select * from usersinfo limit 10";
        ResultSet resultSet = HiveUtils.queryParameter(cn, hql);
        HiveUtils.show(resultSet);
        HiveUtils.close(resultSet, cn);
    }


}
