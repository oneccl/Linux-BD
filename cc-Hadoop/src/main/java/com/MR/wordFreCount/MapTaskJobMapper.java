package com.MR.wordFreCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/14
 * Time: 22:05
 * Description:
 */

// MapTask：负责Mapper阶段数据的预处理

// Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
// Hadoop数据类型：org.apache.hadoop.io
/*
输入：KEYIN：输入Key类型：每一行的偏移量  VALUEIN：输出Value类型：每一行的内容
输出：KEYOUT：输出的Key类型            VALUEOUT：输出的Value类型
*/
// 例：a b a c ==> <a,1> <b,1> <a,1> <c,1>
public class MapTaskJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // map()方法按行读取：
    // LongWritable key：该行偏移量
    // Text value：该行的内容
    // Context context：MapTask输出结果
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString()
                .toLowerCase()
                .replaceAll("\\W", " ")
                .split("\\s+");
        for (String w : line) {
            if (!w.isEmpty()){
                // Mapper阶段输出结果：例：<a,[1,1]> <b,[1]> <c,[1]>
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

}
