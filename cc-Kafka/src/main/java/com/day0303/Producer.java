package com.day0303;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 15:06
 * Description:
 */
public class Producer {

    // 生产者（发送消息/数据）

    public static void main(String[] args) {
        // 0、配置连接集群和K-V序列化
        Properties prop = new Properties();
        // k-V 序列化
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 连接集群的地址
        prop.setProperty("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092");

        // 1、创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        // 2、创建生产者记录对象
        // topic:主题 partition:指定分区(不写为轮循) key: 消息的键(存储value的分类依据信息) value:消息/数据
        // public ProducerRecord(String topic, Integer partition, K key, V value) {}
        ProducerRecord<String, String> record =
                new ProducerRecord<>("t2","k3","v3");
        // 3、创建回调对象，接收上传后Kafka返回的响应结果（同步）
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                String builder = "topic: " + metadata.topic() + "\t" +
                        "partition: " + metadata.partition() + "\t" +
                        "offset: " + metadata.offset();
                System.out.println(builder);
            }
        };
        // 4、发送数据
        Future<RecordMetadata> future = producer.send(record, callback);
        producer.flush();
        // 5、释放资源
        producer.close();

    }

}
