package com.MR.wordFreCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/14
 * Time: 22:20
 * Description:
 */
// MRAppMaster：负责Driver阶段整个程序的过程调度及状态协调
public class MRAppMasterJobDriver {

    @Test
    public void driver() throws Exception {
        // 1、创建Job对象
        Job job = Job.getInstance();
        // 设置MapReduce任务类的过程调度及状态协调
        // 2、Mapper阶段：执行MapTask
        job.setMapperClass(MapTaskJobMapper.class);
        // 设置Mapper阶段的输出Key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper阶段的输出Value类型
        job.setMapOutputValueClass(IntWritable.class);
        // 3、Reduce阶段：执行ReduceTask
        job.setReducerClass(ReduceTaskJobReduce.class);
        // 设置Reduce阶段的最终输出Key类型
        job.setOutputKeyClass(Text.class);
        // 设置Reduce阶段的最终输出Value类型
        job.setOutputValueClass(LongWritable.class);
        // 4、Driver阶段：执行驱动类
        job.setJarByClass(MRAppMasterJobDriver.class);
        // 5、数据的输入与输出
        // 设置数据输入路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt"));
        // 设置结果输出路径
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\Word999"));
        // 6、执行并退出
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
