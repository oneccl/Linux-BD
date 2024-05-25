package com.day0303;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 16:23
 * Description:
 */
public class Consumer {

    // 消费者（接收/获取数据）

    public static void main(String[] args) {
        // 0、配置连接集群和K-V序列化
        Properties prop = new Properties();
        // k-V 反序列化
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 连接集群的地址
        prop.setProperty("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092");
        // 设置消费组(若多个消费者使用相同的group.id，则他们共享一份消费进度的记录)
        prop.setProperty("group.id","g6");
        // 重置偏移量(不实时获取，获取存入的历史数据)
        prop.setProperty("auto.offset.reset","earliest");
        // 手动提交offset，默认true
//        prop.setProperty("enable.auto.commit","false");

        // 1、创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        // 2、订阅/指定主题
        consumer.subscribe(Arrays.asList("t1"));

        // 3、开始读取所有分区数据
        while (true){
            // 4、获取/拉取1次数据 poll(timeout)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                String builder = "topic: " + record.topic() + "\t" +
                        "partition: " + record.partition() + "\t" +
                        "offset: " + record.offset() + "\t" +
                        "K-V: " + record.key() + ":" + record.value();
                System.out.println(builder);
            }
            // 5、读完所有分区数据
            if (records.isEmpty()){
                break;
            }
        }

    }

}
