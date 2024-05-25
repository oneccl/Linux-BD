package com.MR.inputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/18
 * Time: 17:17
 * Description:
 */

// 对合并后的日志文件(sequence类型)进行IP统计
public class JobLogMergeIPCountDriver {
    public static void main(String[] args) throws Exception {
        // 单文件：通过Configuration对象配置参数minSize和maxSize修改分片大小：200M
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize",String.valueOf(100*1024*1024));
        Job job = Job.getInstance(conf);
        // 1、Mapper阶段
        job.setMapperClass(JobLogMergeIPCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 2、Reduce阶段
        job.setReducerClass(JobLogMergeIPCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 3、Driver阶段
        job.setJarByClass(JobLogMergeIPCountDriver.class);
        // 4、设置输入文件格式（合并后的文件类型为sequence）
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 5、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\myLogMerge\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\fileAllIpCount"));
        // 6、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段
// Text：输入文件名 Text：输入文件内容  Text：输出key类型  IntWritable：输出Value类型
class JobLogMergeIPCountMapper extends Mapper<Text,Text,Text, IntWritable>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 内容按\n分割为行
        String[] lines = value.toString().split("\n");
        for (String line : lines) {
            // 行按空格分割取到IP
            String ip = line.split(" ")[0];
            context.write(new Text(ip),new IntWritable(1));
        }
    }
}

// Reduce阶段
class JobLogMergeIPCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
