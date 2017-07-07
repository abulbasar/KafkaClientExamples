package com.einext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSource {


    private static ProducerRecord<String, String> producerRecord;
    private static Properties properties = PropertiesLoader.getKafkaProperties();
    private static String topic = properties.getProperty("topic");
    private static Producer<String, String> producer = new KafkaProducer<>(properties);



    public static void send(String key, String message){
        producerRecord = new ProducerRecord<>(topic, key, message);
        producer.send(producerRecord);
    }
}
