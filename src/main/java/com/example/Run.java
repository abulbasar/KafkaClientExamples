package com.example;

import com.example.common.SchemaUtils;
import com.example.consumer.KafkaAvroSink;
import com.example.producer.KafkaAvroSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Run {

    private static Logger logger = LoggerFactory.getLogger(Run.class.getName());

    private static void runProducer() throws IOException {

        SchemaUtils schemaUtils = new SchemaUtils("Transaction.avsc");
        Schema schema = schemaUtils.getSchema();
        logger.info("Schema: ", schema, schema == null);


        KafkaAvroSource kafka = new KafkaAvroSource("demo");
        for(int i = 0; i < 3; ++i){
            GenericRecord transaction = new GenericData.Record(schema);
            transaction.put("f2", String.format("message %d", i));
            byte[] bytes = schemaUtils.serialize(transaction);
            kafka.send(bytes);
        }
        kafka.close();
    }

    private static void runConsumer() throws IOException{
        KafkaAvroSink kafka = new KafkaAvroSink();
        kafka.subscribe("demo");
        kafka.close();
    }

    public static void main(String[] args) throws IOException{
        if(args[0].equalsIgnoreCase("producer")){
            runProducer();
        }
        if(args[0].equalsIgnoreCase("consumer")){
            runConsumer();
        }
    }
}
