package com.MR.inputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/17
 * Time: 17:32
 * Description:
 */

// 方式2：自定义输入文件合并格式化类
// 继承FileInputFormat类，实现createRecordReader()方法
// 自定义读取器类，继承RecordReader类，实现6个方法

// 合并格式化类
// Text：每个文件的文件名；Text：每个文件的内容
public class FileInputMergeFormat extends FileInputFormat<Text,Text> {
    /**
     * 记录读取器
     * @param inputSplit 分片(每个小文件)
     * @param context 当前任务的上下文对象(包含Job对象)
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // 创建读取器对象，将K，V传递给读取器处理
        FileInputMergeReader mergeReader = new FileInputMergeReader();
        mergeReader.initialize(inputSplit,context);
        return mergeReader;
    }

}

// 读取器类
// Text：每个文件的文件名；Text：每个文件的内容
class FileInputMergeReader extends RecordReader<Text,Text>{

    FileSplit fileSplit = null;
    FileSystem fs = null;
    Text key = new Text();
    Text val = new Text();
    Boolean flag = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // InputSplit(切片)==>文件：可获取文件的相关信息
        fileSplit = (FileSplit) inputSplit;
        // 获取文件管理对象：可以获取输入流读取文件
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
    }

    // 遍历每个切片文件
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag){
            // 获取文件路径
            Path path = fileSplit.getPath();
            // 获取输入流读取文件
            FSDataInputStream fis = fs.open(path);
            // 获取文件名
            String name = path.getName();
            // 获取切片中的文件(小文件单独成片)
            FileStatus fileStatus = fs.listStatus(path)[0];
            // 获取文件大小
            long len = fileStatus.getLen();
            // 一次性读取
            byte[] buffer = new byte[(int) len];
            fis.read(buffer);
            fis.close();
            // key：文件名
            key.set(name);
            // value：文件内容
            val.set(new String(buffer));
            flag = false;
            return true;
        }
        return false;
    }

    // 获取每个文件的key(文件名)
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    // 获取每个文件的value(内容)
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.val;
    }

    // 获取nextKeyValue()每次进程执行的状态
    @Override
    public float getProgress() throws IOException, InterruptedException {
        // flag：当前进程执行的状态0%~100%，nextKeyValue()执行成功flag=false
        return this.flag?0:1;
    }

    // 释放资源
    @Override
    public void close() throws IOException {
        fs.close();
    }

}


