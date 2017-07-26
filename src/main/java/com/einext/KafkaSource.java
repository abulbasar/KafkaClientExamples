package com.einext;

import com.einext.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSource {

    private static Properties properties = PropertiesLoader.loadProperties("kafka.properties");
    private static String topic = properties.getProperty("topic");
    private static Producer<String, String> producer = new KafkaProducer<>(properties);

    public static void send(String key, String message){
        ProducerRecord<String, String> producerRecord; producerRecord = new ProducerRecord<>(topic, key, message);
        producer.send(producerRecord);
    }
}
