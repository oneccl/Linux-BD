package com.MR.outputFormat;

import com.utils.MRUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/31
 * Time: 13:57
 * Description:
 */

// 自定义输出格式化类(ClassifyOutputFormat类)

// 使用MR任务，获得IP的归属地，并将相同归属地的IP放在同一个文件中，命名为:IP所属地.txt
public class JobIPClassifyAreaDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("out.path","C:\\Users\\cc\\Desktop\\IPClassifyAreaFormat");
        Job job = Job.getInstance(conf);
        // 1、Mapper阶段
        job.setMapperClass(JobIPClassifyAreaMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 2、Reduce阶段
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 3、Driver阶段
        job.setJarByClass(JobIPClassifyAreaDriver.class);
        // 4、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\logLess.txt"));
        FileOutputFormat.setOutputPath(job,new Path(conf.get("out.path")));

        // 5、设置输出格式（自定义输出格式）
        job.setOutputFormatClass(ClassifyOutputFormat.class);

        // 6、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段
class JobIPClassifyAreaMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取IP
        String ip = value.toString().split(" ")[0];
        // 获取IP归属地
        String fileName = MRUtil.getIPArea(ip.trim());
        // Text K：IP归属地 Text V：IP
        context.write(new Text(fileName),new Text(ip));
    }
}
