package com.example.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class JsonConsumerCustomSeek {

    private static Logger log = LoggerFactory.getLogger(JsonConsumerCustomSeek.class);

    private KafkaConsumer<String, String> consumer;

    public JsonConsumerCustomSeek(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "JsonConsumer");
        props.put("auto.offset.reset", "latest");

        /* Enable following two properties for auto commit */
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "2000");

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
        try {
            for (TopicPartition partition: consumer.assignment()){
                consumer.seek(partition, 100);
            }


            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, key: %s, message = %s"
                            , record.topic(), record.partition(), record.offset(),
                            record.key(), record.value())
                        );
                }

            }
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args){
        String topicName = "demo";
        JsonConsumerCustomSeek jsonConsumer = new JsonConsumerCustomSeek(topicName);
        jsonConsumer.run();
    }



}
