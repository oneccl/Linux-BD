package com.MR.logStatistics;

import com.utils.MRUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * Date: 2023/1/17
 * Time: 8:56
 * Description:
 */

// Driver阶段
public class JobLogDetailsSortDriver {
    @Test
    public void driver() throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobLogDetailsSortMapper.class);
        job.setMapOutputKeyClass(LogDetails.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 2、Driver阶段
        job.setJarByClass(JobLogDetailsSortDriver.class);
        // 3、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\log1\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\log2"));
        // 4、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段
class JobLogDetailsSortMapper extends Mapper<LongWritable, Text, LogDetails, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        // 上一次MapReduce输出为<null,<IP,count,code200,upload,download>>
        String ip = values[0];
        Long count = MRUtil.longTreat(values[1]);
        Long code200 = MRUtil.longTreat(values[2]);
        Long upload = MRUtil.longTreat(values[3]);
        Long download = MRUtil.longTreat(values[4]);
        context.write( new LogDetails(ip, count, code200, upload, download),NullWritable.get());
    }
}
