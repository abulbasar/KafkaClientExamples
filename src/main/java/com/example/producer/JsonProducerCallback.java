package com.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonProducerCallback implements Callback {

    private static Logger logger = LoggerFactory.getLogger(JsonProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null){
            logger.error(e.getMessage(), e);
        }
        if(recordMetadata != null) {
            logger.debug(String.format("%s %d", recordMetadata.topic(), recordMetadata.offset()));
        }
    }
}
