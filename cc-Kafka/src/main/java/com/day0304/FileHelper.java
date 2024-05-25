package com.day0304;

import com.day0303.BaseHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 17:42
 * Description:
 */

public class FileHelper extends BaseHelper {

    // 初始化配置文件
    public Properties init(String src) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(src));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    // 发送文件
    public void put(String src, String inPath) {
        File file = new File(inPath);
        try {
            FileInputStream inputStream = new FileInputStream(file);
            byte[] data = new byte[2 * 1024 * 1024];
            long sumLen = file.length();
            int ava = inputStream.available();
            while (inputStream.read(data)!=-1){
                long pos=sumLen-ava;
                String key = file.getName()+":"+pos+":"+file.length();
                System.out.println(key);
                produce(src,key,data);
                ava = inputStream.available();
                if (ava>0 && ava<data.length){
                    data=new byte[ava];
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void produce(String src,String key,byte[] data) {
        Properties prop = init(src);
        String topic = prop.getProperty("kafka.topic");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(prop);
        ProducerRecord<String, byte[]> record =
                // public ProducerRecord(String topic, K key, V value) {}
                new ProducerRecord<>(topic,key,data);
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                String res = metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset();
                System.out.println(res);
            }
        };
        producer.send(record,callback);
        producer.flush();
    }

    // 获取文件
    public void consume(String src,String outPath) {
        Properties prop = init(src);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(prop);
        String topic = prop.getProperty("kafka.topic");
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, byte[]> record : records) {
                String key = record.key();
                System.out.println(key);
                String[] fields = key.split(":");
                String fileName = fields[0];
                String offset = fields[1];
                String fileLen = fields[2];
                byte[] value = record.value();
                //get(key,value,outPath+File.separator+fileName);
            }
//            if (records.isEmpty()){
//                break;
//            }
        }

    }

    public void get(String offset, byte[] value, String outPath) {
        try {
            // 参数1：目标文件 参数2：模式，对文件的权限，rw允许读写操作
            // public RandomAccessFile(String name, String mode) {}
            RandomAccessFile accessFile = new RandomAccessFile(outPath,"rw");
            // 数据写入文件指定位置
            accessFile.seek(Long.parseLong(offset));
            accessFile.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void destroy() {

    }

}
