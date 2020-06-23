package com.example.models;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AvroUtils {

    public static Schema getSchema(String fileName) throws IOException {
        InputStream resourceAsStream = AvroUtils.class.getClassLoader().getResourceAsStream(fileName);
        Schema schema = new Schema.Parser().parse(resourceAsStream);
        return schema;
    }

    public static byte[] write(List<GenericData.Record> records, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)
                .create(schema, outputStream);

        for (GenericData.Record record : records) {
            writer.append(record);
        }
        writer.flush();
        writer.close();
        return outputStream.toByteArray();
    }

    public static List<GenericRecord> read(byte[] bytes, Schema schema) throws IOException {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        SeekableByteArrayInput input = new SeekableByteArrayInput(bytes);
        DataFileReader<GenericRecord> genericRecords = new DataFileReader<>(input, datumReader);
        Iterator<GenericRecord> iterator = genericRecords.iterator();

        List<GenericRecord> records = new ArrayList<>();
        while (iterator.hasNext()){
            records.add(iterator.next());
        }
        return records;
    }

    public static Schema getTimestampType(){
        Schema type = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        return type;
    }

    public static Schema getDateType(){
        Schema type = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        return type;
    }
    public static Schema getUUIDType(){
        Schema type = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

        return type;
    }

    public static byte [] toByteArray(Schema schema, GenericRecord genericRecord) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        return baos.toByteArray();
    }


    public static GenericRecord deserialize(Schema schema, byte[] data) throws IOException {
        final GenericData genericData = new GenericData();
        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, genericData);
        return reader.read(null, decoder);
    }

}
