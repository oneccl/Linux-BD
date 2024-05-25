package com.MR.logStatistics;

import com.utils.MRUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/16
 * Time: 16:49
 * Description:
 */

// Driver阶段
public class JobLogDetailsDriver {
    @Test
    public void driver() throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        job.setMapperClass(JobLogDetailsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogDetails.class);
        // 2、Reduce阶段
        job.setReducerClass(JobLogDetailsReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LogDetails.class);
        // 3、Driver阶段
        job.setJarByClass(JobLogDetailsDriver.class);
        // 4、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\log1"));
        // 5、执行
        System.out.println(job.waitForCompletion(true));
    }
}

// Mapper阶段 <偏移量,line> ==> <IP,logDetails>
class JobLogDetailsMapper extends Mapper<LongWritable, Text, Text, LogDetails>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // <IP,logDetails> ==> <IP,<IP,1,code200,upload,download>>
        String[] line = value.toString().split(" ");
        String ip = line[0];
        // 每个IP统计1次
        Long count = 1L;
        // 响应码为200统计1次
        Long code200 = line[8].equals("200")?1L:0;
        Long upload = MRUtil.longTreat(line[9]);
        Long download = MRUtil.longTreat(line[line.length-1]);
        LogDetails logDetails = new LogDetails(ip, count, code200, upload, download);
        context.write(new Text(ip),logDetails);
    }
}

// Reduce阶段 <IP,<IP,1,code200,upload,download>> ==> <null,[logDet0,logDet1,...]>
class JobLogDetailsReduce extends Reducer<Text, LogDetails, NullWritable, LogDetails>{
    @Override
    protected void reduce(Text key, Iterable<LogDetails> values, Context context) throws IOException, InterruptedException {
        LogDetails newLogDetails = new LogDetails();
        for (LogDetails value : values) {
            // 求和计算需要在LogDetails类中实现
            newLogDetails.sum(value);
        }
        context.write(NullWritable.get(),newLogDetails);
    }
}

// JavaBean：用于封装数据
// WritableComparable：Hadoop提供的序列化和比较器接口
class LogDetails implements WritableComparable<LogDetails> {

    private String ip;  // IP
    private long count;  // IP次数
    private long code200;  // 响应码为200的次数
    private long upload;  // 上传流量
    private long download;  // 下载流量

    public LogDetails() {
    }

    public LogDetails(String ip, Long count, Long code200, Long upload, Long download) {
        this.ip = ip;
        this.count = count;
        this.code200 = code200;
        this.upload = upload;
        this.download = download;
    }

    public String getIp() {
        return ip;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }
    public Long getCount() {
        return count;
    }
    public void setCount(Long count) {
        this.count = count;
    }
    public Long getCode200() {
        return code200;
    }
    public void setCode200(Long code200) {
        this.code200 = code200;
    }
    public Long getUpload() {
        return upload;
    }
    public void setUpload(Long upload) {
        this.upload = upload;
    }
    public Long getDownload() {
        return download;
    }
    public void setDownload(Long download) {
        this.download = download;
    }

    // 重写toString()
    @Override
    public String toString() {
        return ip + "\t" + count + "\t" + code200 + "\t" + upload + "\t" + download;
    }

    // 数据计算求和
    public void sum(LogDetails obj){
        this.ip = obj.getIp();
        this.count += obj.getCount();
        this.code200 += obj.getCode200();
        this.upload += obj.getUpload();
        this.download += obj.getDownload();
    }

    // Hadoop序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.ip);
        dataOutput.writeLong(this.count);
        dataOutput.writeLong(this.code200);
        dataOutput.writeLong(this.upload);
        dataOutput.writeLong(this.download);
    }

    // Hadoop反序列化：需与序列化顺序一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ip = dataInput.readUTF();
        this.count = dataInput.readLong();
        this.code200 = dataInput.readLong();
        this.upload = dataInput.readLong();
        this.download = dataInput.readLong();
    }

    // Hadoop比较器
    // 按IP出现的次数排序
    @Override
    public int compareTo(LogDetails o) {
        return Long.compare(o.getCount(),this.count);
    }

}
