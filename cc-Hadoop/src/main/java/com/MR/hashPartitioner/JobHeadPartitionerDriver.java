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

// 方式2：自定义分区器
// 对合并后的日志文件(sequence类型)进行IP统计并分区，按IP的首字符分区
/*
1）分区数 = ReduceTask数，理想状态
2）分区数 < ReduceTask数，活少人多报错
3）分区数 > ReduceTask数，活多人少生成多余空文件
*/
public class JobHeadPartitionerDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobHeadPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 2、Reduce阶段
        job.setReducerClass(JobHeadPartitionerReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 方式2：
        // 3、设置分区器
        job.setPartitionerClass(HeadPartitioner.class);
        // 设置分区数(ReduceTask的数量)
        job.setNumReduceTasks(9);

        // 4、Driver阶段
        job.setJarByClass(JobHeadPartitionerDriver.class);
        // 5、设置输入文件格式（合并后的文件类型为sequence）
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 6、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\myLogMerge\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\myIpCountPartitioner"));
        // 7、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段
// Text：输入文件名 Text：输入文件内容  Text：输出key类型  IntWritable：输出Value类型
class JobHeadPartitionerMapper extends Mapper<Text,Text,Text, IntWritable>{
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
class JobHeadPartitionerReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
