package com.day0303;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/31
 * Time: 19:22
 * Description:
 */
public class KafkaTransaction {

    // Kafka数据一致性 Kafka事务
    /*
    方案1：幂等性：同一操作发起多次请求的结果是一致的(去重)（适用场景：Consumer接收）
    方案2：Kafka事务（适用场景：Read-Process-Write(发送-Topic-接收)、Producer发送）
    */

    public static void main(String[] args) {
        // Kafka事务
        /*
        Read-Process-Write模式：将多条消息生产和消费封装在一个事务中，形成一个原子性操作
        要么全都成功，要么全都失败
        当事务中仅存在Consumer消费操作时，它和Consumer手动提交offset没有区别
        */
        // 事务配置
        /*
        1、Producer:
        需要设置transactional.id属性（设置该属性后，enable.idempotence(幂等性)属性会自动设置为true）
        2、Consumer:
        需要设置isolation.level=read_committed（设置该属性后，Consumer只会读取已提交的事务）
        需要设置enable.auto.commit=false关闭自动提交offset功能
        */
        // Kafka事务案例

        Producer<String, String> producer = getProducer();
        Consumer<String, String> consumer = getConsumer();

        // 1、初始化事务
        producer.initTransactions();
        while (true){
            try {
                // 2、开启事务
                producer.beginTransaction();
                // 3、创建Map集合，用于保存分区对应的offset
                HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()){
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    // 4、保存偏移量
                    offsets.put(
                            new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()+1)
                    );
                    // 消息
                    String msg = record.value();
                    /* 模拟异常 int i = 1/0 */
                    // 5、生产者发送消息到指定主题
                    producer.send(new ProducerRecord<>("topic",msg));
                }
                // 6、提交offset到事务
                producer.sendOffsetsToTransaction(offsets,"group");
                // 7、提交事务
                producer.commitTransaction();
            } catch (ProducerFencedException e) {
                // 8、中断事务（放弃事务）
                producer.abortTransaction();
            }
        }
        /*
        结论：如果中间出现异常，offset不会被提交，除非生产消费消息都成功，才会提交事务
        原理：
        1、事务管理中的事务日志必不可少，Kafka使用一个内部topic来保存事务日志
        事务日志是Transaction Coordinator(事务协调器)管理的状态的持久化，只保存最近的事务状态
        2、Producer宕机重启或漂移到其它机器需要关联之前未完成事务，所以需要有一个唯一标识来进行关联
        就是TransactionalId，一个Producer挂了，另一个有相同TransactionalId的producer
        能够接着处理这个事务未完成的状态
        3、Kafka通过一个名为_consumer_offsets的内部topic来记录offset commit，消息仅在
        其offset被提交给_consumer_offsets时才被认为成功消费；跨多个主题和分区的写入都是原子性的
        */
    }

    // Producer
    // 生产者可能会给多个topic、多个partition发消息，这些消息放到一个事务里，就形成了分布式事务
    public static Producer<String,String> getProducer(){
        // 1、Producer配置
        Properties prop = new Properties();
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092");
        // 指定transactional.id
        prop.put("transactional.id","pro-con-offset-commit");
        // 2、创建Producer对象
        return new KafkaProducer<>(prop);
    }

    // Consumer
    public static Consumer<String,String> getConsumer(){
        // 1、Consumer配置
        Properties prop = new Properties();
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092");
        prop.setProperty("group.id","group");
        // 手动提交offset
        prop.setProperty("enable.auto.commit","false");
        // 读已提交
        prop.setProperty("isolation.level","read_committed");
        // 2、创建Consumer对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        // 3、订阅要消费的主题
        consumer.subscribe(Arrays.asList("topic"));
        return consumer;
    }

}
