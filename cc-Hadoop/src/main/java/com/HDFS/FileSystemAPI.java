package com.HDFS;

import com.utils.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/10
 * Time: 19:02
 * Description:
 */
public class FileSystemAPI {
    // HDFS：分布式文件管理系统
    // Java API：FileSystem操作HDFS

    private FileSystem fs;

    @Before
    public void hdfsOpInit() throws Exception {
        // 1、创建URI路径对象，连接到HDFS分布式文件管理系统
        URI uri = new URI("hdfs://bd91:8020");
        // 2、创建配置文件类对象，可以添加一些配置信息
        Configuration config = new Configuration();
        // 3、创建文件管理FileSystem对象，关于HDFS的所有命令都会封装为FileSystem类下的方法
        // user：HDFS用户名；对象fs相当于命令：hdfs dfs
        fs = FileSystem.get(uri, config, "root");
        // 4、执行操作
    }

    // (1)获取路径下所有文件
    @Test
    public void getFilesTest() throws IOException {
        ArrayList<String> list = new ArrayList<>();
        List<String> files = HDFSUtil.getFiles(fs, list, "/");
        files.forEach(System.out::println);
    }

    // (2)获取hdfs dfs -ls path下所有目录/文件信息(全左对齐或右对齐)
    @Test
    public void getLsAlignTest() throws IOException {
        System.out.println(HDFSUtil.getLsAlign(fs, "/", 5, true));
    }

    // (3)获取hdfs dfs -ls path下所有文件信息(仅getLen()列右对齐)
    @Test
    public void getLsFileTest() throws IOException {
        System.out.println(HDFSUtil.getLsFile(fs, "/test"));
    }

}
