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


/*
$ java -cp target/KafkaClients-0.1-jar-with-dependencies.jar com.example.retail.RetailProducer 1000 10 ../online-retail-dataset.csv

* */

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
        int count = 0;
        try {
            reader = new BufferedReader(new FileReader(this.sourceFile));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
                this.send(this.topicName, null, line);
                if(this.delayBetweenMessage > 0){
                    Thread.sleep(delayBetweenMessage);
                }
                if(count > this.countOfMessages && this.countOfMessages>0){
                    break;
                }
                ++count;
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {

        int delayBetweenMessages = 200; //millisecond
        int countOfMessage = 10;
        String sourceFile = args[0];

        if (args.length != 3) {
            System.out.println("Usage: java -cp <jar> com.example.producer.RetailProducer <delay> <count> <source file>");
            System.exit(0);
        }
        countOfMessage = Integer.valueOf(args[1]);
        delayBetweenMessages = Integer.valueOf(args[0]);
        sourceFile = args[2];

        new RetailProducer(countOfMessage, delayBetweenMessages, sourceFile).start();
    }
}
