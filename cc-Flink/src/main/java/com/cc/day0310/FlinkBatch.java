package com.cc.day0310;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 16:51
 * Description:
 */

public class FlinkBatch {

    // BatchJob Flink批处理

    public static void main(String[] args) throws Exception {
        // 1、创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2、获取批处理对象：DataSet
        DataSet<String> ds = env.readTextFile("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt");

        // 3、处理：单词统计
        DataSet<Tuple2<String, Integer>> ds1 = ds
                // 扁平化
                .flatMap(new MyFlatMap())
                // 过滤
                .filter(t -> !t.f0.isEmpty())
                // 元组可根据第1个字段分组
                .groupBy(0)
                // 根据元组第2个字段聚合
                .sum(1)
                // 设置单分区
                .setParallelism(1)
                // 根据元组第2个字段降序
                .sortPartition(1, Order.DESCENDING);

        // 4、输出
        ds1.print();
    }
}

// <String,Tuple2<String,Integer> ==> <输入,输出>
class MyFlatMap implements FlatMapFunction<String,Tuple2<String,Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = line.toLowerCase()
                .replaceAll("\\W+", " ")
                .split("\\s+");
        for (String word : words) {
            out.collect(new Tuple2<>(word,1));
        }
    }
}