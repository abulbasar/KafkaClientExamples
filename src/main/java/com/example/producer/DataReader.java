package com.example.producer;

import java.io.*;
import java.util.stream.Stream;

public class DataReader {

    public static Stream<String> getRecords(String fileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        return bufferedReader.lines();
    }
}
