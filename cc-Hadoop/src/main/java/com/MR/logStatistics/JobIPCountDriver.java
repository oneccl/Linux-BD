package com.MR.logStatistics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/16
 * Time: 14:42
 * Description:
 */
// 统计单个log文件中的IP次数
public class JobIPCountDriver {

    @Test
    public void driver() throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobIPCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 2、Reduce阶段
        job.setReducerClass(JobIPCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 3、Driver阶段
        job.setJarByClass(JobIPCountDriver.class);
        // 4、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\IP1"));
        // 5、执行
        System.out.println(job.waitForCompletion(true));
    }

}

// Mapper阶段：MapTask：获取<IP,1>...
class JobIPCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String ip = value.toString().split(" ")[0];
        context.write(new Text(ip),new IntWritable(1));
    }
}

// Reduce阶段：ReduceTask：计算聚合
class JobIPCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}