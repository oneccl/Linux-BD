package com.MR.wordFreCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/17
 * Time: 20:31
 * Description:
 */
public class WordCountPackage {

    // Java项目打包：
    /*
    1）添加SpringBoot项目打包依赖
    2）开启hdfs集群（start-dfs.sh）
    3）提交到集群执行：
    hadoop jar cc-Hadoop-1.0-SNAPSHOT.jar  jar包
    com.MR.wordFreCount.WordCountPackage   主类（Main-Class）
    /Harry.txt  hdfs://bd91:8020/MROut     输入路径（args(0)） 输出路径（args(1)）
    */
    // 指定主类：
    /*
    方式1：命令行指定
    方式2：打开打好的jar包cc-Hadoop-1.0-SNAPSHOT.jar\META-INF\MANIFEST.MF，添加Main-Class
    方式3：打包前在pom.xml中配置
    */

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        // 2、Mapper阶段
        job.setMapperClass(MapTaskJobMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 3、Reduce阶段
        job.setReducerClass(ReduceTaskJobReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 4、Driver阶段
        job.setJarByClass(MRAppMasterJobDriver.class);
        // 5、输入输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }

}
