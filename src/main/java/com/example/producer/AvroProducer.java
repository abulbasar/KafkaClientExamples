package com.example.producer;

import com.example.models.AvroUtils;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class AvroProducer {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private KafkaProducer<String, byte[]> kafkaProducer;
    

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
        props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
        props.put("client.id", clientId);
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void send(String topicName, String key, byte[] value) throws IOException {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, key, value);
        this.kafkaProducer.send(record, new Callback() {
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
        this.kafkaProducer.flush();
        this.kafkaProducer.close();
    }

    public static void main(String[] args) throws Exception {
        String fileName =  "/Users/abasar/data/stocks.csv";
        AvroProducer avroProducer = new AvroProducer();
        avroProducer.start();
        String topic = "stocks-avro";
        Stream<String> records = DataReader.getRecords(fileName).filter(line -> !line.startsWith("date"));
        final long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        ObjectMapper objectMapper = new ObjectMapper();
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

        //final Schema schema = AvroUtils.getSchema("stock.avsc");
        final Schema schema = Stock.getSchema();


        final int BATCH_SIZE = 10000;
        List<GenericData.Record> stockList = new ArrayList<>(BATCH_SIZE);

        records.forEach(line -> {
                    try {
                        Stock stock = Stock.textDeserialization(line);
                        GenericData.Record record = stock.toAvroGeneric(schema);
                        stockList.add(record);
                        int currentCount = count.incrementAndGet();
                        if (currentCount % BATCH_SIZE == 0) {
                            byte[] bytes = AvroUtils.write(stockList, schema);
                            totalByteSize.addAndGet(bytes.length);
                            avroProducer.send(topic, null, bytes);
                            long duration = System.currentTimeMillis() - startTime;
                            long eps = count.get() * 1000 / duration;
                            long avgSize = totalByteSize.get()/count.get();
                            System.out.println(String.format("Complete, sent all lines with eps: %d, avg size: %d"
                                    , eps, avgSize));
                            stockList.clear();
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                });
        avroProducer.close();
        long duration = System.currentTimeMillis() - startTime;
        long eps = count.get() * 1000 / duration;
        System.out.println(String.format("Complete, sent all lines with eps: %d", eps));
    }

}
