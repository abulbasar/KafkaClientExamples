package com.einext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSource {

    private static Producer<String, String> producer;
    private static ProducerRecord<String, String> producerRecord;

    private static String topic;

    {
        Properties properties = PropertiesLoader.getKafkaProperties();
        topic = properties.getProperty("topic");
        properties.remove("topic");
        producer = new KafkaProducer<>(properties);

    }

    public static void send(String key, String message){
        producerRecord = new ProducerRecord<>(topic, key, message);
        producer.send(producerRecord);
    }
}
