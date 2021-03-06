package com.blu3monk3y.kkvstore.util;

/**
 * Created by navery on 18/05/2017.
 */

public class KafkaProperties {
    public static final String TOPIC = "topic1";
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
//    public static final String KAFKA_SERVER_URL = "192.168.99.100";
//    public static final int KAFKA_SERVER_PORT = 32770;

    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";

    private KafkaProperties() {}
}
