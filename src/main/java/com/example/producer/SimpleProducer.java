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

public class SimpleProducer {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaProducer<String, String> stringKafkaProducer;


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
        props.put("value.serializer", StringSerializer.class.getCanonicalName());
        props.put("client.id", clientId);
        stringKafkaProducer = new KafkaProducer<>(props);
    }

    public void send(String topicName, String key, String value){
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
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
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
        String topic = "stocks-json";
        Stream<String> records = DataReader.getRecords(fileName).filter(line -> !line.startsWith("date"));
        final long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        ObjectMapper objectMapper = JsonUtils.getObjectMapper();
        int caseNum = 2;
        AtomicLong totalByteSize = new AtomicLong(0);

        /*
        List<String> jsonRecords = new ArrayList<>();
        records.forEach(line -> {
            try {
                Stock stock = Stock.textDeserialization(line);
                totalByteSize.addAndGet(line.getBytes().length);
                String json = objectMapper.writeValueAsString(stock);
                jsonRecords.add(json);
            }catch (Exception e){
                e.printStackTrace();
            }

        });

        records = jsonRecords.stream();

        */


        records
                .forEach(line -> {

                    try {

                        switch (caseNum){
                            case 1:
                                totalByteSize.addAndGet(line.getBytes().length);
                                simpleProducer.send(topic, null, line);
                                break;
                            case 2:
                                Stock stock = Stock.textDeserialization(line);
                                String json = objectMapper.writeValueAsString(stock);
                                totalByteSize.addAndGet(json.getBytes().length);
                                simpleProducer.send(topic, null, json);
                                break;
                            default:
                                throw new RuntimeException("Unsupported test scenario: " + caseNum);
                        }

                        int currentCount = count.incrementAndGet();
                        if (currentCount % 10000 == 0) {
                            long duration = System.currentTimeMillis() - startTime;
                            long eps = count.get() * 1000 / duration;
                            long avgSize = totalByteSize.get()/count.get();
                            System.out.println(String.format("Complete, sent all lines with eps: %d, avg size: %d"
                                    , eps, avgSize));
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                });
        simpleProducer.close();
        long duration = System.currentTimeMillis() - startTime;
        long eps = count.get() * 1000 / duration;
        System.out.println(String.format("Complete, sent all lines with eps: %d", eps));
    }
}
