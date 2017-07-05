package com.einext;

import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;


public class PropertiesLoader {
    public static Properties getKafkaProperties(){
        Properties props = new Properties();
        String filename = "kafka.properties";
        try {
            InputStream is = PropertiesLoader.class.getClassLoader().getResourceAsStream(filename);
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }
}
