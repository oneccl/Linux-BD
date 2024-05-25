package com.cc.day0314;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/16
 * Time: 9:49
 * Description:
 */
public class Double11CaseJava {

    // Flink案例（Java版）：业务需求：
    // 1、实时统计当天0点到当前时间的销售总额（11-11 00:00:00~11-11 23:59:59）
    // 2、统计销售额排名前3的类别
    // 3、每秒更新一次结果

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行模式自动
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // Source输入：自定义数据源，获取Flink实时处理对象
        DataStream<Tuple2<String, Double>> ds = env.addSource(new MySourceJava());
        //ds.print();
        // Transformation：数据处理
        DataStream<CategoryJava> ds1 = ds.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                //public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
                //     AggregateFunction<T, ACC, V> aggFunction,
                //     WindowFunction<V, R, K, W> windowFunction) {}
                .aggregate(new MyAggregateJava(), new MyWindowJava());
        //ds1.print();

        // Sink输出：实时统计销售总额和类别销售额排名前3
        // public <R> SingleOutputStreamOperator<R> process(ProcessWindowFunction<T, R, K, W> function) {}
        DataStream<Object> ds2 = ds1.keyBy(CategoryJava::getDt)  // 根据时间分组，统计窗口1s
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                // 自定义输出形式
                .process(new MyProcessJava());

        //ds2.addSink();

        // 懒执行，开启任务
        env.execute("Double11CaseJava");
    }

}

// 自定义累加器：<IN, ACC, OUT> => <输入，累加器，输出>
class MyAggregateJava implements AggregateFunction<Tuple2<String,Double>, Double, Double> {
    // 累加器初始值
    @Override
    public Double createAccumulator() {
        return 0d;
    }
    // 当前结果与历史累加器结果聚合
    @Override
    public Double add(Tuple2<String, Double> value, Double acc) {
        return value.f1+acc;
    }
    // 返回累加器结果
    @Override
    public Double getResult(Double acc) {
        return acc;
    }
    // 多个分区累加器结果聚合
    @Override
    public Double merge(Double acc1, Double acc2) {
        return acc1+acc2;
    }
}

// 自定义转换输出：<IN, OUT, KEY, W extends Window> => <输入，输出，分组key，窗口类型>
class MyWindowJava implements WindowFunction<Double, CategoryJava, String, TimeWindow> {
    // 转换当前窗口的元组内容为CategoryJava对象
    @Override
    public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryJava> out) throws Exception {
        Double ctgTotal = input.iterator().next();
        String dt = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss").format(new Date());
        out.collect(new CategoryJava(key,ctgTotal,dt));
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class CategoryJava{
    private String ctg;
    private Double ctgTotal;
    private String dt;
}

// 自定义处理输出：<IN, OUT, KEY, W extends Window> => <输入，输出，分组key，窗口类型>
class MyProcessJava extends ProcessWindowFunction<CategoryJava, Object, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<CategoryJava> elements, Collector<Object> out) throws Exception {
        // 1、统计总销售额
        Double total = 0d;
        total+=elements.iterator().next().getCtgTotal();
        String formatTotal = new DecimalFormat("0.00").format(total);
        System.out.println("-------------------------------");
        String dt = key.split(" ")[1];
        System.out.println("时间: "+dt);
        System.out.println("总销售额: "+formatTotal);
        // 2、统计销售额排名前3的类别
        // Iterable<T>类型 => Stream<T>类型
        Stream<CategoryJava> stream = StreamSupport.stream(elements.spliterator(), false);
        System.out.println("排名\t\t类别\t\t销售额");
        AtomicInteger count = new AtomicInteger();  // 原子性变量
        // 排序并反转降序
        stream.sorted(Comparator.comparing(CategoryJava::getCtgTotal).reversed())
                .limit(3)  // 取前3
                .forEach(o->{
                    count.getAndIncrement();  // 保证变量原子性
                    String ctg = o.getCtg();
                    String ctgTotal = new DecimalFormat("0.00").format(o.getCtgTotal());
                    System.out.println(count+"\t\t"+ctg+"\t\t"+ctgTotal);
                });

    }
}
