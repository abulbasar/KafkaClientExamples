package com.einext.common;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class SchemaUtils {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Injection<GenericRecord, byte[]> injection = null;
    private Schema schema = null;

    public SchemaUtils(String schemaFile) throws IOException {

        InputStream stream = getClass().getClassLoader().getResourceAsStream(schemaFile);
        if(stream != null){
            String str = IOUtils.toString(stream, "UTF-8");
            logger.debug("Schema (string): " + str);
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(str);
            injection = GenericAvroCodecs.toBinary(schema);
        }else{
            logger.info("No schema file (Transaction.avsc) is found");
        }
    }

    public Schema getSchema(){
        logger.debug("Schema", schema);
        return schema;
    }

    public GenericRecord deserialize(byte[] bytes){
        return injection.invert(bytes).get();
    }

    public byte[] serialize(GenericRecord record){
        return injection.apply(record);
    }

}
