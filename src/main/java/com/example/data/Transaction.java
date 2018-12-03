package com.example.data;

import org.codehaus.jackson.map.ObjectMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;


public class Transaction {
    private static ObjectMapper mapper = new ObjectMapper();

    private String tnxId;
    private String sourceId;
    private String targetId;
    private Long timestamp; // EpochMilli
    private Double amount;

    public Transaction(){
        this.tnxId = UUID.randomUUID().toString();
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static void setMapper(ObjectMapper mapper) {
        Transaction.mapper = mapper;
    }

    public String getTnxId() {
        return tnxId;
    }

    public void setTnxId(String tnxId) {
        this.tnxId = tnxId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public static List<Transaction> randomGenerate(int count){
        List<Transaction> records = new ArrayList<>();
        Random random = new Random();
        Instant now = Instant.now();

        int[] sources = random.ints (count, 0, 1000).toArray();
        int[] targets = random.ints(count, 0, 1000).toArray();
        int[] timeDeltas = random.ints(count, 0, 100).toArray();
        double[] amounts = random.doubles(count, 0.1, 10000.0).toArray();


        for(int i = 0; i< count; ++i){
            Transaction record = new Transaction();
            record.amount = amounts[i];
            record.timestamp = now.plusMillis(timeDeltas[i]).toEpochMilli();
            record.sourceId = String.format("%010d", sources[i]);
            record.targetId = String.format("%010d", targets[i]);
            records.add(record);
        }
        return records;
    }



    public String toJson(){
        String result = null;
        try{
            result = mapper.writeValueAsString(this);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }


    /*
    public static void main(String[] args){
        for(Transaction t: Transaction.randomGenerate(10)){
            System.out.println(t.toJson());
        }
    }*/

}
