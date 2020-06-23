package com.example.producer;

import com.example.JsonUtils;
import com.example.models.Stock;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StockJsonSerializer implements Serializer<Stock> {

    private ObjectMapper objectMapper;
    private ObjectWriter objectWriter;

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.objectMapper = JsonUtils.getObjectMapper();
        this.objectWriter = this.objectMapper.writerFor(Stock.class);
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Stock data) {
        return serialize(topic, null, data);
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic   topic associated with data
     * @param headers headers associated with the record
     * @param data    typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Headers headers, Stock data) {
        try {
            return this.objectWriter.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Close this serializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    public void close() {
        this.objectMapper = null;
        this.objectWriter = null;

    }
}
