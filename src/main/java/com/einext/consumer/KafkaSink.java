package com.einext.consumer;

import com.einext.common.SchemaUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaSink {

    private KafkaConsumer<String, byte[]> consumer = null;
    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    private SchemaUtils schemaUtils = null;

    public KafkaSink() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("auto.offset.reset", "latest");
        props.put("group.id", "console-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
        this.consumer = new KafkaConsumer<>(props);
        schemaUtils = new SchemaUtils("Transaction.avsc");
    }

    public void subscribe(String topic){
        consumer.subscribe(Collections.singletonList(topic));
        while (true){
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            records.forEach(record -> {
                try{
                    GenericRecord transaction = schemaUtils.deserialize(record.value());
                    System.out.println(transaction);
                }catch (Throwable throwable){
                    throwable.printStackTrace();
                }
            });
        }
    }

    public void close(){
        logger.info("Closing the connection to kafka broker");
        consumer.close();
    }
}
