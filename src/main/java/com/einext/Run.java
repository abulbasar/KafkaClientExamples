package com.einext;

import com.einext.common.SchemaUtils;
import com.einext.consumer.KafkaSink;
import com.einext.producer.KafkaSource;
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


        KafkaSource kafka = new KafkaSource("sample");
        for(int i = 0; i < 3; ++i){
            GenericRecord transaction = new GenericData.Record(schema);
            transaction.put("f2", String.format("abul %d", i));
            byte[] bytes = schemaUtils.serialize(transaction);
            kafka.send(bytes);
        }
        kafka.close();
    }

    private static void runConsumer() throws IOException{
        KafkaSink kafka = new KafkaSink();
        kafka.subscribe("sample");
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
