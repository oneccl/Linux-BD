package com.MR.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/31
 * Time: 16:57
 * Description:
 */

// MR输出格式化
// MR任务默认使用TextOutputFormat，将K和V toString后使用\t分割，每个KV对输出到一行中

// 自定义输出格式化类
// 自定义类继承FileOutputFormat，重写getRecordWriter()方法
// 自定义类继承RecordWriter，重写write(K key,V value)方法、close()方法
public class ClassifyOutputFormat extends FileOutputFormat<Text,Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 自定义记录写出器类
        return new ClassifyRecordWriter(job);
    }
}

// 记录写出器
// Text:输出K(所属地)  Text:输出V(IP)
class ClassifyRecordWriter extends RecordWriter<Text,Text>{

    String basePath;
    FileOutputStream fos;

    public ClassifyRecordWriter(TaskAttemptContext job) {
        Configuration conf = job.getConfiguration();
        // 动态获取文件输出的基路径(Driver中以配置)
        basePath = conf.get("out.path");
    }

    // Text text:输出K(所属地)  Text value:输出V(IP)
    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        String fileName = key.toString();
        String ip = value.toString();
        System.out.println("****** "+ fileName +"\t"+ ip);
        // 文件输出路径
        String outPath = basePath+"/"+fileName+".txt";
        // 文件输出流，追加写入(不指定将覆盖)
        fos = new FileOutputStream(outPath,true);
        // 每写一个IP进行换行
        fos.write((ip+"\n").getBytes(StandardCharsets.UTF_8));
    }

    // 释放资源
    @Override
    public void close(TaskAttemptContext job) throws IOException, InterruptedException {
        fos.close();
    }

}
