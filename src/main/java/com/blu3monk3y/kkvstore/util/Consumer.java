package com.blu3monk3y.kkvstore.util;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class Consumer<K, V> extends ShutdownableThread {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    private static int UID =1;
    private final String consumerGroup;
    private int id;
    private KafkaConsumer<K, V> consumer;
    private final String topic;
    int msgs;
    private String server = "none";
    private int port = -1;

    public Consumer(String topic, String consumerGroup) {
        super("Consumer." + topic + "." + consumerGroup +"." + UID++, false);
        this.id = UID -1;
        this.consumerGroup = consumerGroup;
        log("Created");
        this.topic = topic;
    }
    public Consumer create() {

        if (server.equals("none")) throw new RuntimeException("Server not specified, i.e. producer(...).withServer(" + KafkaProperties.KAFKA_SERVER_URL + ")");
        if (port == -1) throw new RuntimeException("Port not specified");

        log("reading data from:" + this.topic);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server + ":" + port);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(this.topic));
        this.start();
        return this;
    }
    /**
     * KafkaProperties.KAFKA_SERVER_URL
     */
    public Consumer withServer(String server) {
        this.server = server;
        return this;
    }
    /**
     * KafkaProperties.KAFKA_SERVER_PORT
     */
    public Consumer withPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Callback to client code
     * @param topic
     * @param partition
     * @param timestamp
     * @param key
     * @param value
     * @param offset
     */
    int handled = 0;
    public void handle(String topic, int partition, long timestamp, K key, V value, long offset) {
        log(" Topic:" + topic + " partition:" + partition + " time:" + new Date(timestamp) +  " handled:" + handled++ + " Msg: (" + key + ", " + value + ") at offset " + offset);
    }

    @Override
    public void doWork() {
        ConsumerRecords<K, V> records = consumer.poll(1000);
        for (ConsumerRecord<K, V> record : records) {
            handle(record.topic(), record.partition(), record.timestamp(), record.key(), record.value(), record.offset());
            msgs++;
        }
    }

    public int msgs() {
        return msgs;
    }

    public void shutdown() {
        // dodgy on state handling?
        super.shutdown();
    }

    @Override
    protected void shutdownClientCode() {
        log("Unsubscribe consumer");
        consumer.unsubscribe();
        consumer.close();
    }

    public void log(String msg) {
        log.info(msg);
    }
}
