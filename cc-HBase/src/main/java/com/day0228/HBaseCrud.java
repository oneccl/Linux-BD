package com.day0228;

import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/28
 * Time: 18:51
 * Description:
 */
public class HBaseCrud {

    // HBase JavaAPI CRUD

    @Test
    public void hbaseCrudTest(){
        // 1、连接HBase
        System.out.println(HBaseCrudUtil.cn);
        // 2、获取HBase集群管理对象
        System.out.println(HBaseCrudUtil.admin);
        // 3、显示所有表
        HBaseCrudUtil.list();
        // 4、显示所有命名空间(HBase数据库)
        HBaseCrudUtil.listNamespace();
        // 5、命名空间是否存在
        System.out.println(HBaseCrudUtil.isNamespaceExists("hbase"));
        // 6、创建命名空间(HBase数据库)
        System.out.println(HBaseCrudUtil.createNamespace("test"));
        // 7、表是否存在
        System.out.println(HBaseCrudUtil.isTableExists("t1"));
        // 8、创建表
        System.out.println(HBaseCrudUtil.createTable("t2", "f3", "f4"));
        // 9、获取表对象
        Table stu = HBaseCrudUtil.getTable("stu");
        Table t1 = HBaseCrudUtil.getTable("t1");
        // 10、添加/修改表数据
        Map<String, Object> data = new HashMap<>();
        data.put("user","root");
        data.put("pw","123456");
        System.out.println(HBaseCrudUtil.put("t1", "1000", "f1", data));
        // 11、扫描/查询rowKey数据
        System.out.println(HBaseCrudUtil.getByRowKey("stu","1001"));
        // 12、扫描/查询表所有数据
        System.out.println(HBaseCrudUtil.get("stu"));
        // 13、删除表指定rowKey数据
        System.out.println(HBaseCrudUtil.delByRowKey("stu","1000"));
        // 14、删除表
        System.out.println(HBaseCrudUtil.delTable("t2"));
        // 15、删除命名空间(HBase数据库)
        System.out.println(HBaseCrudUtil.delNamespace("test"));
    }

}
