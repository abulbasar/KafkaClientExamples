package com.example.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class JsonConsumer {

    private static Logger log = LoggerFactory.getLogger(JsonConsumer.class);

    private KafkaConsumer<String, String> consumer;

    public JsonConsumer(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "JsonConsumer");
        props.put("auto.offset.reset", "latest");

        /* Enable following two properties for auto commit */
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "2000");

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
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, key: %s, message = %s"
                            , record.topic(), record.partition(), record.offset(),
                            record.key(), record.value())
                        );
                }
                try{
                    /* CommitSync retries committing as long as there is no error that can’t be recovered.
                    If this happens, there is not much we can do except log an error. One drawback of manual commit
                    is that the application is blocked until the broker responds to the commit request.
                    */
                    consumer.commitSync();


                    /* The drawback is that while commitSync() will retry the commit until it either succeeds
                     or encounters a nonretriable failure, commitAsync() will not retry. The reason it does not retry
                     is that by the time commitAsync() receives a response from the server,
                     there may have been a later commit that was already successful. */

                    //consumer.commitAsync();
                }catch (CommitFailedException ex){
                    log.error(ex.getMessage(), ex);
                }
            }
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args){
        String topicName = "demo";
        JsonConsumer jsonConsumer = new JsonConsumer(topicName);
        jsonConsumer.run();
    }



}
