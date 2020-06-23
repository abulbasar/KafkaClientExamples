package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

    public static ObjectMapper getObjectMapper(){
        return new ObjectMapper();
    }
}
