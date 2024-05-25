package com.MR.combinerAndCounter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
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

// Combiner：是MR提供的一个Map端的预聚合机制
// 在Map输出之后，没有产生网络shuffle之前，在Map端对数据进行类似Reducer的聚合操作
// 网络shuffle：通过网络跨机器传输(拉取每个机器MapTask结束后的<K,1>)，效率较低

// Counter：MR程序中，分布式运行过程中会启动若干个Map/Reduce Task(线程)
// 可以通过MR提供的计数器Counter统计整个任务的某些环节的执行次数或数据量

// 对合并后的日志文件(sequence类型)进行IP统计
public class JobIPCountCombinerDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobIPCountCombinerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Combiner预聚合:
        // 2、设置预聚合
        job.setCombinerClass(JobAddCombiner.class);

        // 3、Reduce阶段
        job.setReducerClass(JobIPCountCombinerReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 4、Driver阶段
        job.setJarByClass(JobIPCountCombinerDriver.class);
        // 5、设置输入文件格式（合并后的文件类型为sequence）
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 6、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\myLogMerge\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\IpCountCombiner"));
        // 7、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Combiner预聚合:
// 添加一个预聚合
class JobAddCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}

// Mapper阶段
// Text：输入文件名 Text：输入文件内容  Text：输出key类型  IntWritable：输出Value类型
class JobIPCountCombinerMapper extends Mapper<Text,Text,Text, IntWritable>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 内容按\n分割为行
        String[] lines = value.toString().split("\n");

        // Counter计数器：参数1：组名 参数2：计数器名
        Counter counter = context.getCounter("统计", "MapTask执行的次数");
        // 每执行一次加1，计数结果在运行日志中显示
        counter.increment(1);

        for (String line : lines) {
            // 行按空格分割取到IP
            String ip = line.split(" ")[0];
            context.write(new Text(ip),new IntWritable(1));
        }
    }
}

// Reduce阶段
class JobIPCountCombinerReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
