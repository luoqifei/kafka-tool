package com.luoqifei.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducerClient {
    public static final Logger LOG = LoggerFactory.getLogger(MyProducerClient.class);
    private Producer<String, String> producer;
    private String topicName = "";
    private Properties props;

    public MyProducerClient(String topicName, String serverAddress) throws ExecutionException, InterruptedException {
        this.topicName = topicName;
        props = new Properties();
        props.put("bootstrap.servers", serverAddress);
        initDefaultProps();
        this.producer = new KafkaProducer<String, String>(props);
        testKafka();
    }

    private void testKafka() throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<String, String>("test", "health test", "health test")).get();
    }

    private void initDefaultProps() {
        //props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "1");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 3);
        //props.put("producer.type", "async");
        //wait for all server broker ack in 20s,if not,return timeout,but not contain the net refuse
        props.put("timeout.ms", 20000);
        //Reduce the no of requests less than 0
        //props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
    }

    /**
     * @deprecated use sendMessageSync or sendMessageAsync
     */
    public void sendMessage(String key, String values) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<String, String>(topicName, key, values));
    }

    public void sendMessageSync(String key, String values) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<String, String>(topicName, key, values)).get();
    }

    public void sendMessageAsync(String key, String values) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<String, String>(topicName, key, values));
    }

    public void close() {
        if (this.producer != null) {
            producer.close();
        }
    }

    public static void main(String[] args) throws Exception {
//        if (args.length == 0) {
//            System.out.println("Enter topic name");
//            return;
//        }
        String topicName = "test";
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "127.0.0.1:9095");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);
        for (int i = 0; i < 100000000; i++) {
            System.out.println(i);
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
            Thread.sleep(100);
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
