package com.example.producer;

import com.example.JsonUtils;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class StockProducer {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaProducer<String, Stock> stringKafkaProducer;


    public void start(){

        String clientId = "demo-client-1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("retries", Integer.MAX_VALUE);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put("batch.size", 300);
        props.put("compression.type", "snappy");
        props.put("linger.ms", 1000);
        props.put("buffer.memory", 32 * 1024 * 1024);
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", StockJsonSerializer.class.getName());
        props.put("client.id", clientId);
        stringKafkaProducer = new KafkaProducer<>(props);
    }

    public void send(String topicName, String key, Stock value){
        ProducerRecord<String, Stock> record = new ProducerRecord<>(topicName, key, value);
        this.stringKafkaProducer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    logger.error("Failed to send", e);
                }
                /*System.out.println(String.format("key: %s, value: %s, Offset: %d"
                        , key, value, recordMetadata.offset()));*/
            }
        });
    }

    public void close(){
        this.stringKafkaProducer.flush();
        this.stringKafkaProducer.close();
    }

    public static void main(String[] args) throws Exception {
        String fileName =  "/Users/abasar/data/stocks.csv";
        StockProducer simpleProducer = new StockProducer();
        simpleProducer.start();
        String topic = "stocks-json";
        Stream<String> records = DataReader.getRecords(fileName).filter(line -> !line.startsWith("date"));
        final long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        records.forEach(line -> {
                final Stock stock = Stock.textDeserialization(line);
                simpleProducer.send(topic, null, stock);
                int currentCount = count.incrementAndGet();
                if (currentCount % 10000 == 0) {
                    long duration = System.currentTimeMillis() - startTime;
                    long eps = count.get() * 1000 / duration;
                    System.out.println(String.format("Complete, sent all lines with eps: %d", eps));
                }
        });
        simpleProducer.close();
        long duration = System.currentTimeMillis() - startTime;
        long eps = count.get() * 1000 / duration;
        System.out.println(String.format("Complete, sent all lines with eps: %d", eps));
    }
}
