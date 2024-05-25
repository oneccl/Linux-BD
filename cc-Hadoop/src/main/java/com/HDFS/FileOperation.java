package com.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/11
 * Time: 18:55
 * Description:
 */
public class FileOperation {
    // JavaAPI操作HDFS文件

    FileSystem fs;

    @Before
    public void getFileSystem() throws Exception {
        fs = FileSystem.get(new URI("hdfs://bd91:8020"), new Configuration(), "root");
    }

    // 上传文件
    @Test
    public void put() throws IOException {
        Path src = new Path("C:\\Users\\cc\\Desktop\\p.txt");
        Path dst = new Path("/");
        fs.copyFromLocalFile(src,dst);
    }

    // 下载文件
    @Test
    public void get() throws IOException {
        Path src = new Path("C:\\Users\\cc\\Desktop");
        Path dst = new Path("/b.txt");
        fs.copyToLocalFile(dst,src);
    }

    // 创建目录
    @Test
    public void mkdir() throws IOException {
        System.out.println(fs.mkdirs(new Path("/a")));
    }

    // 删除目录
    @Test
    public void rm() throws IOException {
        // 参数2：是否递归
        System.out.println(fs.delete(new Path("/a"), true));
    }

    // 修改文件名
    @Test
    public void mv() throws IOException {
        System.out.println(fs.rename(new Path("/x.txt"), new Path("/b.txt")));
    }

    // 使用IO流上传文件
    // 本地文件-->输入流-->Java客户端-->输出流-->HDFS
    @Test
    public void putByIO() throws Exception {
        // 1、输入流
        FileInputStream fis = new FileInputStream("C:\\Users\\cc\\Desktop\\p.txt");
        // 2、获取输出流
        FSDataOutputStream fos = fs.create(new Path("/p.txt"));
        // 3、读取并上传
        // 1)自定义
//        byte[] buffer = new byte[1024];
//        int read = fis.read(buffer);
//        while (read >= 0){
//            fos.write(buffer);
//            read = fis.read(buffer);
//            int available = fis.available();
//            if (available < buffer.length){
//                buffer = new byte[available];
//            }
//        }
        // 2)使用提供的工具类
        IOUtils.copyBytes(fis,fos,1024);
    }

    // 使用IO流下载文件
    // HDFS-->输入流-->Java客户端-->输出流-->本地文件
    @Test
    public void getByIO() throws IOException {
        // 1、输入流
        FSDataInputStream fis = fs.open(new Path("/x.txt"));
        // 2、输出流
        FileOutputStream fos = new FileOutputStream("C:\\Users\\cc\\Desktop\\x.txt");
        // 3、读取并下载
        IOUtils.copyBytes(fis,fos,1024);
    }

}
