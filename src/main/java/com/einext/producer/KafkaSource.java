package com.einext.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class KafkaSource {

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final UUID producerId;
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());


    public KafkaSource(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 256);
        props.put("compression.type", "snappy");
        props.put("linger.ms", 1);
        props.put("buffer.memory", 32 * 1024 * 1024);
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());

        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        this.topic = topic;

        producerId = UUID.randomUUID();
        logger.info("ProducerId: ", producerId);
    }

    public void send(byte[] bytes){
        try{
            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(this.topic, bytes);
            producer.send(producerRecord);
        }catch (Throwable throwable){
            throwable.printStackTrace();
        }
    }
    public void close(){
        logger.info("Closing the connection to kafka broker");
        producer.close();
    }
}
