package com.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JsonConsumerCustomCommit {

    private static Logger log = LoggerFactory.getLogger(JsonConsumerCustomCommit.class);

    private KafkaConsumer<String, String> consumer;

    public JsonConsumerCustomCommit(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "JsonConsumer");
        props.put("auto.offset.reset", "latest");

        props.put("enable.auto.commit", "false");

        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put("max.poll.records", "1000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton(topic));

    }

    public void run(){

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        long counter = 0;
        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, key: %s, message = %s"
                            , record.topic(), record.partition(), record.offset(),
                            record.key(), record.value()));
                    counter ++;

                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset(), "no metadata");

                    currentOffsets.put(topicPartition, offsetAndMetadata);

                    if(counter % 100 == 0){
                        consumer.commitAsync(currentOffsets, null);
                    }
                }
            }
        }finally {
            try{
                consumer.commitSync();
            }finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args){
        String topicName = "demo";
        JsonConsumerCustomCommit jsonConsumer = new JsonConsumerCustomCommit(topicName);
        jsonConsumer.run();
    }



}
