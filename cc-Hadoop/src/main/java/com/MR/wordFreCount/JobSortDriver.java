package com.MR.wordFreCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/16
 * Time: 14:15
 * Description:
 */
// 按出现次数排序，交换第一次MapReduce结果的K,V
public class JobSortDriver {

    @Test
    public void sortDriver() throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段：MapTask交换任务
        job.setMapperClass(JobSortMapper.class);
        // Mapper阶段输出K,V类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 2、Driver阶段：驱动执行
        job.setJarByClass(JobSortDriver.class);
        // 3、文件输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\Word1\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\Word2"));
        // 4、执行
        System.out.println(job.waitForCompletion(true));
    }

}

// Mapper阶段：MapTask：交换K,V，使用单词出现的次数排序
class JobSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // MapReduce结果默认使用\t分隔
        String[] strings = value.toString().split("\t");
        // K,V交换输出
        long count = Long.parseLong(strings[1]);
        context.write(new LongWritable(count),new Text(strings[0]));
    }

}
