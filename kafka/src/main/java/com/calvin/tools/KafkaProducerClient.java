package com.calvin.tools;

import com.alibaba.fastjson2.JSON;
import com.calvin.tools.data.TestData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class KafkaProducerClient {


    public static void main(String[] args) {
        Properties props = buildProperties();
        KafkaProducer<String, String> producer = new KafkaProducer(props);

        TestData testData = new TestData();
        for (int i = 0; i < 10000000; i++) {
           testData.setId((long) i);
           testData.setCreateTime(new Date());
           if(i%20==0){
               testData.setId((long) (i-7));
               testData.setField1("juJZLZjWkUucNlXJHQxE1Aqcj"+System.currentTimeMillis());
        }
           String jsonData = JSON.toJSONString(testData);
           send("test", jsonData, producer);
        }


    }

    private static void send(String topic, String jsonData, KafkaProducer producer) {
        ProducerRecord<String, String> record = new ProducerRecord(topic, jsonData);
        producer.send(record);
    }


    private static Properties buildProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("bootstrap.servers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
