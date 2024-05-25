package com.cc.day0310;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 17:21
 * Description:
 */

public class FlinkStreaming {

    // StreamingJob Flink实时处理/流处理

    public static void main(String[] args) throws Exception {
        // 1、获取Flink实时处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2、获取实时处理对象
        DataStream<String> ds = env.socketTextStream("bd91", 8888);

        // 3、处理
        DataStream<Tuple2<String, Integer>> ds1 = ds
                .flatMap(new MyFlatMap())
                .filter(t->!t.f0.isEmpty())
                .keyBy(t -> t.f0)
                .sum(1);

        // 4、输出
        ds1.print();

        // 5、执行：开启任务
        env.execute("DataStream");

    }

}
