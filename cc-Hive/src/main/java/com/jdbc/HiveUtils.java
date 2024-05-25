package com.jdbc;

import com.sun.istack.internal.NotNull;

import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/10/8
 * Time: 21:33
 * Description:
 */
@HiveConfig()
public class HiveUtils {

    // 使用注解的方式连接数据库
    // cla：当前类的class对象
    public static <T> Connection connect(@NotNull Class<T> cla) {
        // 使用当前类的class对象
        // 根据注解接口的class对象获取注解的对象
        HiveConfig annotation = cla.getDeclaredAnnotation(HiveConfig.class);
        try {
            // 加载驱动
            Class.forName(annotation.driver());
            // 创建连接
            return DriverManager.getConnection(annotation.url(), annotation.user(), annotation.pw());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // 通用 查询
    public static ResultSet queryParameter(Connection cn, String sql){
        PreparedStatement ps = null;
        ResultSet re = null;
        try {
            ps = cn.prepareStatement(sql);
            re = ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return re;
    }

    // 遍历查询结果
    public static void show(ResultSet resSet) {
        try {
            // 获取ResultSet对象中列的类型或属性的对象
            ResultSetMetaData columns = resSet.getMetaData();
            while (resSet.next()){
                String res = "";
                // 获取列数 getColumnCount()
                for (int i = 0; i < columns.getColumnCount(); i++) {
                    if (i == 0){
                        // 获取列名 getColumnName()
                        // 获取列名对应的值 getString(列名)
                        res = resSet.getString(columns.getColumnName(i+1));
                    }else {
                        res = res +"\t"+ resSet.getString(columns.getColumnName(i+1));
                    }
                }
                System.out.println(res);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // DQL 释放资源
    public static void close(ResultSet re, Statement st, Connection cn){
        if (re != null && st != null && cn != null){
            try {
                re.close();
                st.close();
                cn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 其他调用 释放资源
    public static void close(ResultSet re,Connection cn){
        if (re != null && cn != null){
            try {
                re.close();
                cn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
