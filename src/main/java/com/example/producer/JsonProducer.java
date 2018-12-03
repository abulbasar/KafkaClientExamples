package com.example.producer;


import com.example.data.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

public class JsonProducer {

    private static Logger logger = LoggerFactory.getLogger(JsonProducer.class.getName());
    private final KafkaProducer producer;
    private ProducerRecord producerRecord;
    private int delayBetweenMessage = -1; // Milliseconds
    private int countOfMessages = 10;


    public JsonProducer(int countOfMessages, int delayBetweenMessage) {

        this.delayBetweenMessage = delayBetweenMessage;
        this.countOfMessages = countOfMessages;

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

    public void run() {

        logger.info(String.format("Delay between messages: %d, count: %d"
                , delayBetweenMessage, countOfMessages));

        Instant start = Instant.now();

        for (Transaction t : Transaction.randomGenerate(countOfMessages)) {
            try {
                send("demo", t.getSourceId(), t.toJson());
                if(delayBetweenMessage>0) {
                    Thread.sleep(delayBetweenMessage);
                }
            } catch (InterruptedException ex) {
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        Instant end = Instant.now();
        long duration = end.toEpochMilli() - start.toEpochMilli();
        logger.info(String.format("Total time taken: %d", duration));
        close();
    }

    public static void main(String[] args) {

        int delayBetweenMessages = 200; //millisecond
        int countOfMessage = 10;

        if (args.length < 2) {
            System.out.println("Usage: java -cp <jar> com.example.producer.JsonProducer <message count per thread> <delay ms>");
            System.exit(0);
        }
        countOfMessage = Integer.valueOf(args[0]);
        delayBetweenMessages = Integer.valueOf(args[1]);

        JsonProducer jsonProducer = new JsonProducer(countOfMessage, delayBetweenMessages);

        jsonProducer.run();

    }
}
