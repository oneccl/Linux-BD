package com.MR.wordFreCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/14
 * Time: 22:21
 * Description:
 */

// ReduceTask：负责reduce阶段的数据聚合处理

// Reducer<Text, IntWritable,Text, LongWritable>
/*
输入：Text：MapTask的输出Key类型  IntWritable：MapTask的输出Value类型
输出：Text：最终结果的Key类型      LongWritable：最终结果的Value类型
*/
// 例：<a,[1,1]> <b,[1]> <c,[1]> ==> <a,2> <b,1> <c,1>
public class ReduceTaskJobReduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    // ReduceTask进程对每一组相同的K的<K,V>组调用一次reduce()方法
    // Text key：MapTask任务输出结果的Key类型
    // Iterable<IntWritable> values：MapTask任务输出结果的Value类型(Collection)
    // Context context：ReduceTask输出最终结果
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0L;
        for (IntWritable value : values) {
            count += value.get();
        }
        // Reduce阶段输出聚合结果
        context.write(key,new LongWritable(count));
    }

}
