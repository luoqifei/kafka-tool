package com.luoqifei.kafka.client;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private KafkaConsumer<String, String> consumer;
    private String topic;
    private String bootstrapServer = "";
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Consumer(Properties properties) {
        consumer = new KafkaConsumer<String, String>(properties);
    }
    public Consumer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "count-message-number");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
    }
    public Consumer(Properties properties, String topic) {
        consumer = new KafkaConsumer<String, String>(properties);
        this.topic = topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
    public void setBootstrapServer(String bootstrapServer){
        this.bootstrapServer = bootstrapServer;
    }
    public Map<TopicPartition, Long> getEndOffsets() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        return consumer.endOffsets(topicPartitions);
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "count-message-number");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer consumer = new Consumer(properties);
        String topicName = "test";
        consumer.setTopic(topicName);
        long start = 1543126500000L;
        long end = 1543126660000L;

        logger.info(">>>>  There are " + consumer.countMessageNumber(start, end)
                + " messages between " + start + " and " + end + " timestamp.");
    }

    public long countMessageNumber(long start, long end) {
        long result = 0;
        if (start >= end) {
            logger.error("[startTime]" + start + " >= [endTime]+" + end + " .");
            return result;
        }
        logger.info(">>>>  Start timestamp " + start + " = " + df.format(start));
        logger.info(">>>>  End timestamp " + end + " = " + df.format(end));
        try {
            // get topic's partition info
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = new ArrayList<>();

            Map<TopicPartition, Long> startTimestampsToSearch = new HashMap<>();
            Map<TopicPartition, Long> endTimestampsToSearch = new HashMap<>();


            // set timestamp to every partition by timestamps
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                startTimestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), start);
                endTimestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), end);
            }

            consumer.assign(topicPartitions);

            /*
            Look up the offsets for the given partitions by timestamp.
            The returned offset for each partition is the earliest offset
            whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
            */
            Map<TopicPartition, OffsetAndTimestamp> startTimestampMap = consumer.offsetsForTimes(startTimestampsToSearch);
            Map<TopicPartition, OffsetAndTimestamp> endTimestampMap = consumer.offsetsForTimes(endTimestampsToSearch);


            logger.info(">>>>  Now we get every partition's closest offset before these timestamp area as follow ...");
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : startTimestampMap.entrySet()) {
                OffsetAndTimestamp offsetTimestamp = entry.getValue();
                /* if the timestamp we set for look up offset greater than the largest offset's timestamp,
                    it will return null
                */
                if (offsetTimestamp != null) {
                    int partition = entry.getKey().partition();
                    long startTimestamp = offsetTimestamp.timestamp();
                    long startOffset = offsetTimestamp.offset();

                    logger.info("  ****** partition-" + partition + ", start-timestamp = " + df.format(new Date(startTimestamp)) +
                            ", offset = " + startOffset);
                    //get this partition-endTimestamp's map
                    OffsetAndTimestamp endOffsetAndTimestamp = endTimestampMap.get(entry.getKey());
                    long dValue = 0;
                    if (endOffsetAndTimestamp == null) {
                        logger.warn("  ****** Warn : end time stamp great than the latest time stamp.");
                        Map<TopicPartition, Long> endOffsetMap = getEndOffsets();
                        long endOffset = endOffsetMap.get(entry.getKey());
                        logger.info("  ****** partition-" + partition + "latest offset is " + endOffset);
                        dValue = (endOffset - startOffset);
                    } else {
                        long endTimestamp = endOffsetAndTimestamp.timestamp();
                        long endOffset = endOffsetAndTimestamp.offset();
                        logger.info("  ****** partition = " + partition + ", end-timestamp = " + df.format(new Date(endTimestamp)) +
                                ", offset = " + endOffset);
                        //add the message's number by offset d_value
                        dValue = (endTimestampMap.get(entry.getKey()).offset() - startOffset);
                    }
                    logger.info("   ###### partition-" + partition + " has " + dValue + " messages between timestamp " + start + " and " + end);
                    result += dValue;
                    // set offset to get message
                    //consumer.seek(entry.getKey(), startOffset);
                } else {
                    logger.error(">>>>  The start time stamp great than the latest time stamp.");
                    return 0;
                }
            }

            logger.info(">>>>  Finish get offset for every partitions...");

        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }
        logger.info(">>>>  There are " + result
                + " messages between " + start + " and " + end + " timestamp.");
        return result;
    }
}
