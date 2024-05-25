package com.day0228;

import com.sun.istack.internal.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/28
 * Time: 14:11
 * Description:
 */
public class HBaseCrudUtil {

    private static final String name = "hbase.zookeeper.quorum";
    private static final String val = "bd91:2181,bd92:2181,bd93:2181";

    public static final Connection cn;
    public static final Admin admin;

    static {
        cn = getConnect(name,val);
        admin = getAdmin();
    }

    // 获取HBase连接
    public static Connection getConnect(String name,String val){
        try {
            // 配置连接HBase的zk集群
            Configuration conf = new Configuration();
            conf.set(name,val);
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 获取HBase集群管理对象
    public static Admin getAdmin(){
        try {
            if (cn!=null){
                return cn.getAdmin();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // show tables
    public static void list(){
        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 显示所有命名空间(HBase数据库)
    public static void listNamespace(){
        try {
            NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespace : namespaces) {
                System.out.println(namespace.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建命名空间(HBase数据库)
    public static Boolean createNamespace(String namespace){
        try {
            if (isNamespaceExists(namespace)){
                return false;
            }
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 命名空间是否存在
    public static Boolean isNamespaceExists(String namespace){
        try {
            NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor ns : namespaces) {
                if (ns.getName().equals(namespace)){
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 创建表（最大版本数:500:相同rowKey的不同版本数量,下同）
    public static Boolean createTable(String tn,String... fs){
        // 表是否存在
        if (isTableExists(tn)){
            return false;
        }
        // 8.1、表格描述器
        HTableDescriptor td = new HTableDescriptor(tn);
        // 8.2、创建列族，添加列族
        for (String f : fs) {
            td.addFamily(new HColumnDescriptor(f).setVersions(1,500));
        }
        // 8.3、创建表
        try {
            admin.createTable(td);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 表是否存在
    public static Boolean isTableExists(String tn){
        try {
            return admin.tableExists(TableName.valueOf(tn));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 获取表对象
    public static Table getTable(String tn){
        try {
            if (cn!=null && isTableExists(tn)){
                return cn.getTable(TableName.valueOf(tn));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 添加数据
    public static Boolean put(String tn, String rk, String f, Map<String,Object> values){
        // 10.1、指定行键（rowKey策略）
        Put put = new Put(rk.getBytes());
        // 10.2、指定列族添加数据
        for (String key : values.keySet()) {
            put.addColumn(
                    f.getBytes(),     // 列族
                    key.getBytes(),   // 列名
                    values.get(key).toString().getBytes()  // 列值
                    );
        }
        // 10.3、添加数据
        try {
            Table t = getTable(tn);
            if (t!=null)
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 添加数据
    public static Boolean put(Table t, String rk, String f, Map<String,Object> values){
        // 10.1、指定行键（rowKey策略）
        Put put = new Put(rk.getBytes());
        // 10.2、指定列族添加数据
        for (String key : values.keySet()) {
            put.addColumn(
                    f.getBytes(),     // 列族
                    key.getBytes(),   // 列名
                    values.get(key).toString().getBytes()  // 列值
            );
        }
        // 10.3、添加数据
        try {
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 添加数据
    public static Boolean put(String tn, String rk, String f, String col,Object val){
        Put put = new Put(rk.getBytes());
        HashMap<String, Object> values = new HashMap<>();
        values.put(col,val);
        for (String key : values.keySet()) {
            put.addColumn(f.getBytes(), key.getBytes(), values.get(key).toString().getBytes());
        }
        try {
            Table t = getTable(tn);
            if (t!=null)
                t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 添加数据
    public static Boolean put(Table t, String rk, String f, String col,Object val){
        Put put = new Put(rk.getBytes());
        HashMap<String, Object> values = new HashMap<>();
        values.put(col,val);
        for (String key : values.keySet()) {
            put.addColumn(f.getBytes(), key.getBytes(), values.get(key).toString().getBytes());
        }
        try {
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 查询指定rowKey
    public static String getByRowKey(String tn, String rk){
        // 查询
        ResultScanner sc = null;
        try {
            Table t = getTable(tn);
            if (t!=null) {
                sc = t.getScanner(new Scan().setRowPrefixFilter(rk.getBytes()).setMaxVersions(500));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (sc!=null){
            // 获取所有列
            // 方式1：
            StringBuilder builder = new StringBuilder();
            for (Result result : sc) {
                for (Cell cell : result.rawCells()) {
                    builder.append(new String(CellUtil.cloneRow(cell))).append("\t")  // rowKey
                            .append(new String(CellUtil.cloneFamily(cell))).append(":")  // 列族名
                            .append(new String(CellUtil.cloneQualifier(cell))).append("\t") // 列名
                            .append(new String(CellUtil.cloneValue(cell))).append("\n");  // 列值
                }
            }
            // 方式2：
//            List<Cell> cellList = result.listCells();
//            for (Cell cell : cellList) {
//                builder.append(new String(CellUtil.cloneRow(cell))).append("\t")
//                        .append(new String(CellUtil.cloneFamily(cell))).append(":")
//                        .append(new String(CellUtil.cloneQualifier(cell))).append("\t")
//                        .append(new String(CellUtil.cloneValue(cell))).append("\n");
//            }
            return builder.toString();
        }
        return null;
    }

    // 查询表所有数据（可指定范围[sta,stop)）
    public static String get(String tn, Integer... range){
        StringBuilder builder = new StringBuilder();
        try {
            Table t = getTable(tn);
            if (t!=null) {
                Scan scan = new Scan().setMaxVersions(500);
                if (range.length==2)
                for (int i = range[0]; i < range[1]; i++) {
                    scan.setRowPrefixFilter(String.valueOf(i).getBytes());
                }
                ResultScanner sc = t.getScanner(scan);
                // 获取每行数据
                for (Result result : sc) {
                    // 获取每行中的每列数据
                    for (Cell cell : result.listCells()) {
                        builder.append(new String(CellUtil.cloneRow(cell))).append("\t")  // rowKey
                                .append(new String(CellUtil.cloneFamily(cell))).append(":")  // 列族名
                                .append(new String(CellUtil.cloneQualifier(cell))).append("\t") // 列名
                                .append(new String(CellUtil.cloneValue(cell))).append("\n");  // 列值
                    }
                }
                return builder.toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 获取指定列族的指定列数据
    public static List<String> getByQua(String tn, String rk, String f, String qua){
        List<String> list = new ArrayList<>();
        try {
            Table t = getTable(tn);
            Get get = new Get(rk.getBytes());
            get.addColumn(Bytes.toBytes(f),Bytes.toBytes(qua));
            Scan scan = new Scan();
            scan.setRowPrefixFilter(rk.getBytes());
            scan.addColumn(f.getBytes(),qua.getBytes());
            scan.setMaxVersions(500);
            ResultScanner sc = t.getScanner(scan);
            for (Result result : sc) {
                for (Cell cell : result.rawCells()) {
                    list.add(new String(CellUtil.cloneValue(cell)));
                }
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 删除指定rowKey数据
    public static Boolean delByRowKey(String tn, String rk){
        Table t = getTable(tn);
        // 存在则执行
        if (t!=null && isTableExists(t.getName().getNameAsString())){
            try {
                //HTableDescriptor td = admin.getTableDescriptor(TableName.valueOf(tn));
                //td.remove(rk);
                ResultScanner sc = t.getScanner(new Scan().setRowPrefixFilter(rk.getBytes()).setMaxVersions(500));
                for (Result result : sc) {
                    for (Cell cell : result.rawCells()) {
                        String rowKey = new String(CellUtil.cloneRow(cell));
                        Delete delete = new Delete(rowKey.getBytes());
                        t.delete(delete);
                    }
                }
                return true;
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return false;
    }

    // 删除指定列族的rokKey数据
    public static Boolean delByFamRk(String tn, String f, String rk){
        try {
            Table t = getTable(tn);
            if (t!=null && isTableExists(tn)) {
//                Delete delete = new Delete(rk.getBytes());
//                delete.addFamily(f.getBytes());
//                t.delete(delete);
                Scan scan = new Scan().setRowPrefixFilter(rk.getBytes()).setMaxVersions(500);
                ResultScanner sc = t.getScanner(scan);
                for (Result result : sc) {
                    for (Cell cell : result.rawCells()) {
                        String rowKey = new String(CellUtil.cloneRow(cell));
                        Delete delete = new Delete(rowKey.getBytes());
                        delete.addFamily(f.getBytes());
                        t.delete(delete);
                    }
                }
                return true;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    // drop删除表
    public static Boolean delTable(String tn){
        // 存在则执行
        if (isTableExists(tn)){
            try {
                admin.disableTable(TableName.valueOf(tn)); // 禁用
                admin.deleteTable(TableName.valueOf(tn));  // 删除
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    // 删除命名空间(HBase数据库)
    public static Boolean delNamespace(String namespace){
        // 存在则执行
        if (isNamespaceExists(namespace)){
            try {
                admin.deleteNamespace(namespace);
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    // 清空表
    public static Boolean truncate(String tn){
        try {
            admin.disableTable(TableName.valueOf(tn));  // 禁用
            admin.truncateTable(TableName.valueOf(tn),true);  // 清空
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
