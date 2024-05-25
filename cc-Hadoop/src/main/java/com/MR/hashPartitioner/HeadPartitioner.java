package com.MR.hashPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/18
 * Time: 18:25
 * Description:
 */

// 方式2：
// 自定义分区器，继承Partitioner类，重写getPartition()方法
// 仿照HashPartitioner<K, V> extends Partitioner<K, V>

// 按IP的首字符分区
// Text：IP   IntWritable：IP次数
public class HeadPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        // 获取IP的首字符（范围：0~9）
        int ipHead = Integer.parseInt(text.toString().substring(0, 1));
        // 这里只需让分区数umPartitions=ReduceTask=9即可
        return ipHead % numPartitions;
    }
}
