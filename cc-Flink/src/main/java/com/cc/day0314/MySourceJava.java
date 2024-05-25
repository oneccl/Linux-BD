package com.cc.day0314;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/16
 * Time: 9:58
 * Description:
 */
public class MySourceJava implements SourceFunction<Tuple2<String,Double>> {

    List<String> category = Arrays.asList("女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公");
    Random random = new Random();
    Boolean flag = false;

    @Override
    public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
        while (!flag) {
            String ctg = category.get(random.nextInt(category.size()));
            Double price = random.nextDouble() * 100;
            ctx.collect(new Tuple2<>(ctg, price));
            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        this.flag=true;
    }

}
