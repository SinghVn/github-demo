package com.dea.stream.ingest;

import java.util.Properties;

public class KafkaRunner {

    public static  void main(String args[]){

        Properties prop= new Properties();
        prop.setProperty("bootstrap.servers","127.0.1.9092");
        prop.setProperty("key.serializer","127.0.1.9092");
        prop.setProperty("value.serializer","127.0.1.9092");
        prop.setProperty("acks","1");
        prop.setProperty("retries","3");


    }
}
