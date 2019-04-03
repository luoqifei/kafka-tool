package com.luoqifei.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class MyConsumerClient {
    public static final Logger log = LoggerFactory.getLogger(MyConsumerClient.class);
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        properties.put("group.id", "message-number");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String>  consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("test2"));
        try {
            while (true) {
                Thread.sleep(1000);
                ConsumerRecords<String, String> records = consumer.poll(100);  //2)
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    log.info("topic = {}, partition = {}, offset = {}, key={}, value={}", record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        } finally {
            consumer.close(); //4
        }

    }
}