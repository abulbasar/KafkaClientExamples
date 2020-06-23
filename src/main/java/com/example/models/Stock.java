package com.example.models;


import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

@Data
public class Stock {

    private String date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
    private Double adjclose;
    private String symbol;

    public static Stock textDeserialization(String line) {
        String[] split = line.split(",");
        Stock stock = new Stock();
        //stock.date = LocalDate.parse(split[0]);
        stock.date = split[0];
        stock.open = Double.parseDouble(split[1]);
        stock.high = Double.parseDouble(split[2]);
        stock.low = Double.parseDouble(split[3]);
        stock.close = Double.parseDouble(split[4]);
        stock.volume = Double.parseDouble(split[5]);
        stock.adjclose = Double.parseDouble(split[6]);
        stock.symbol = split[7];
        return stock;
    }

    public GenericData.Record toAvroGeneric(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("date", date);
        record.put("open", open);
        record.put("high", high);
        record.put("low", low);
        record.put("close", close);
        record.put("volume", volume);
        record.put("adjclose", adjclose);
        record.put("symbol", symbol);
        return record;
    }

    public static Schema getSchema(){
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                .record(Stock.class.getSimpleName())
                .namespace(Stock.class.getName())
                .fields();
        fields
                .name("date").type().nullable().stringType().noDefault()
                .name("open").type().nullable().doubleType().noDefault()
                .name("high").type().nullable().doubleType().noDefault()
                .name("low").type().nullable().doubleType().noDefault()
                .name("close").type().nullable().doubleType().noDefault()
                .name("volume").type().nullable().doubleType().noDefault()
                .name("adjclose").type().nullable().doubleType().noDefault()
                .name("symbol").type().nullable().stringType().noDefault()
        ;

        return fields.endRecord();
    }

    public static Stock getInstance(GenericRecord record){
        Stock stock = new Stock();
        stock.date = ((Utf8)record.get("date")).toString();
        stock.open = (Double)record.get("open");
        stock.high = (Double)record.get("high");
        stock.low = (Double)record.get("low");
        stock.close = (Double)record.get("close");
        stock.volume = (Double)record.get("volume");
        stock.adjclose = (Double)record.get("adjclose");

        stock.symbol = ((Utf8)record.get("symbol")).toString();
        return stock;
    }
}
