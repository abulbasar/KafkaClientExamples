package com.einext.utils;

import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;


public class PropertiesLoader {
    public static Properties loadProperties(String fileName){
        Properties props = new Properties();
        try {
            InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(fileName);
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }


}
