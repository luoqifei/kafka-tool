package com.luoqifei.kafka;

import com.luoqifei.kafka.client.Consumer;

import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class WelcomeEntrance {
    public static void main(String[] args) throws InterruptedException {
        System.out.println(new Date()+ " | Hello , welcome in kafka-tools world. I am very young but helpful, what can i do as follow : ");
        note:
        while (true){
            System.out.println("[1] count the topic's message number by start timestamp and end timestamp...");
            System.out.println("[0] exist");
            Scanner scanner = new Scanner(System.in);
            int selectOps = scanner.nextInt();
            switch (selectOps) {
                case 1 :
                {
                    System.out.println("Now please input topic's name");
                    String topicName = scanner.next();

                    System.out.println("Now please input start timestamp");
                    long start = scanner.nextLong();

                    System.out.println("Now please input end timestamp");
                    long end = scanner.nextLong();

                    System.out.println("Now input bootstrap servers,for example: localhost1:9092,localhost2:9092");
                    String bootstrapServers = scanner.next();
                    scanner.close();

                    Properties properties = new Properties();
                    properties.put("bootstrap.servers", bootstrapServers);
                    properties.put("group.id", "count-message-number");
                    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                    Consumer consumer = new Consumer(properties);
                    consumer.setTopic(topicName);
                    Thread.sleep(3000);
                    consumer.countMessageNumber(start,end);
                }
                case 0 :
                    System.out.println("Thinks for you use, Have a nice day.");
                    System.exit(1);
                default:
                    System.out.println("Please input an valid options, int type options.like 1,0...");
                    continue note;
            }
        }
    }
}
