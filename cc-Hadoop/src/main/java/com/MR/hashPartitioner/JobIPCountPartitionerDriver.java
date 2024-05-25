package com.MR.hashPartitioner;

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

// MR分区：分成多个文件输出
// MR默认使用HashPartitioner，将Key的hash % reduceTask
// HashPartitioner可以比较均衡的将Map的输出分配给每一个reducer避免数据倾斜

// 方式1：使用默认的分区器HashPartitioner，设置ReduceTask数量)
// 方式2：自定义分区器（见JobHeadPartitionerDriver类、HeadPartitioner类）

// 对合并后的日志文件(sequence类型)进行IP统计并分区
public class JobIPCountPartitionerDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobIPCountPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 2、Reduce阶段
        job.setReducerClass(JobIPCountPartitionerReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 方式1：
        // 3、设置分区数(ReduceTask的数量)
        job.setNumReduceTasks(3);

        // 4、Driver阶段
        job.setJarByClass(JobHeadPartitionerDriver.class);
        // 5、设置输入文件格式（合并后的文件类型为sequence）
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 6、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\myLogMerge\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\IpCountPartitioner"));
        // 7、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段
// Text：输入文件名 Text：输入文件内容  Text：输出key类型  IntWritable：输出Value类型
class JobIPCountPartitionerMapper extends Mapper<Text,Text,Text, IntWritable>{
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
class JobIPCountPartitionerReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
