package com.MR.inputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/17
 * Time: 18:48
 * Description:
 */

// 方式2：自定义分片合并格式化类Driver中设置
public class JobFileInputMergeDriver {
    @Test
    public void driver() throws Exception {
        Job job = Job.getInstance();

        // 1、Mapper阶段(合并文件时Map输出K,V可不设置)
        job.setMapperClass(JobFileInputMergeMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);

        // 方式2：
        // 1)设置输入格式
        job.setInputFormatClass(FileInputMergeFormat.class);
        // 2)设置输出格式：设置输出sequence类型文件(方便处理)
        // sequence文件组成：seq+Text(K类型)+Text(V类型)+文件名1+文件内容1+文件名2+文件内容2+...
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 2、Reduce阶段
        // 设置最终输出K，V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 3、Driver阶段
        job.setJarByClass(JobFileInputMergeDriver.class);
        // 4、输入输出路径：输入路径读取所有小文件所在的目录路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\weblogs"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\myLogMerge"));
        // 5、执行
        //job.submit();
        System.out.println(job.waitForCompletion(true));
    }
}

// 合并文件只需要MapTask即可
// 文件读取时指定Key为Text：表示文件名
/*
Text：输入文件名  Text：输入每个文件内容  Text：输出文件名  Text：输出合并后的文件内容
*/
class JobFileInputMergeMapper extends Mapper<Text,Text,Text,Text>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("文件名: "+key.toString());
        System.out.println("文件大小: "+value.getLength());
        context.write(key,value);
    }
}

