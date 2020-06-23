package com.example.consumer;

import com.example.JsonUtils;
import com.example.models.AvroUtils;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class AvroConsumer {

    private KafkaConsumer<String, byte[]> consumer;
    private AtomicBoolean stopFlag = new AtomicBoolean(false);

    public void init(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "JsonConsumer");
        props.put("auto.offset.reset", "earliest"); // Only applicable new consumer group

        /* Enable following two properties for auto commit */
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "2000");

        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put("max.poll.records", "1000");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer",ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED);

        consumer = new KafkaConsumer<>(props);
    }

    public void start() throws IOException {
        this.init();
        String topic = "stocks-avro";
        consumer.subscribe(Collections.singleton(topic));
        ObjectMapper objectMapper = JsonUtils.getObjectMapper();
        Stock stock  = null;
        final Schema schema = Stock.getSchema();
        while (!stopFlag.get()) {
            long startTime = System.currentTimeMillis();
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
            System.out.println("Actual wait for new messages: " + (System.currentTimeMillis() - startTime));
            int count = consumerRecords.count();
            Iterator<ConsumerRecord<String, byte[]>> iterator = consumerRecords.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, byte[]> message = iterator.next();
                String topic1 = message.topic();
                byte[] bytes = message.value();

                List<GenericRecord> genericRecords = AvroUtils.read(bytes, schema);
                List<Stock> stocks = genericRecords.stream().map(Stock::getInstance).collect(Collectors.toList());
                String key = message.key();
                Headers headers = message.headers();
                long offset = message.offset();
                long timestamp = message.timestamp();
                message.timestampType();
            }
            //consumer.commitSync();
        }

    }


    public static void main(String[] args) throws Exception {
        new AvroConsumer().start();
    }
}
