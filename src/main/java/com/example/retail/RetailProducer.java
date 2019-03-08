package com.example.retail;


import com.example.producer.JsonProducerCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class RetailProducer {

    private static Logger logger = LoggerFactory.getLogger(RetailProducer.class.getName());
    private final KafkaProducer producer;
    private ProducerRecord producerRecord;
    private int delayBetweenMessage = -1; // Milliseconds
    private int countOfMessages = 10;
    private String sourceFile;
    private String topicName;


    public RetailProducer(int countOfMessages, int delayBetweenMessage, String sourceFile) {

        this.delayBetweenMessage = delayBetweenMessage;
        this.countOfMessages = countOfMessages;
        this.sourceFile=sourceFile;
        this.topicName = "retail";

        String clientId = "TransactionProducer";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        //props.put("retries", 3);
        props.put("batch.size", 256);
        props.put("compression.type", "snappy");
        props.put("linger.ms", 1);
        props.put("buffer.memory", 32 * 1024 * 1024);
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", StringSerializer.class.getCanonicalName());
        props.put("client.id", clientId);

        producer = new KafkaProducer<String, String>(props);

        logger.info("clientId: ", clientId);
    }

    private void send(String topic, String key, String message) {
        producerRecord = new ProducerRecord<String, String>(topic, key, message);
        producer.send(producerRecord, new JsonProducerCallback());
    }

    private void close() {
        logger.info("Shutting down the producer");
        producer.flush();
        producer.close();
    }

    private void start(){
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(this.sourceFile));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
                this.send(this.topicName, null, line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {

        int delayBetweenMessages = 200; //millisecond
        int countOfMessage = 10;
        String sourceFile = args[0];

        if (args.length < 2) {
            System.out.println("Usage: java -cp <jar> com.example.producer.RetailProducer <message count per thread> <delay ms>");
            System.exit(0);
        }
        new RetailProducer(countOfMessage, delayBetweenMessages, sourceFile).start();
    }
}
