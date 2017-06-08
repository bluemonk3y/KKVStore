package com.blu3monk3y.kkvstore.util;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by navery on 18/05/2017.
 */
public class Producer<K, V> extends Thread {
    private KafkaProducer<K, V> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int maxMessages;
    private String server = "none";
    private int port = -1;

    public Producer(String topic, Boolean isAsync, int maxMessages) {
        this.maxMessages = maxMessages;
        this.topic = topic;
        this.isAsync = isAsync;
    }
    public Producer create() {
        Properties props = new Properties();
        if (server.equals("none")) throw new RuntimeException("Server not specified, i.e. producer(...).withServer(" + KafkaProperties.KAFKA_SERVER_URL + ")");
        if (port == -1) throw new RuntimeException("Port not specified");

        props.put("bootstrap.servers", server + ":" + port);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        //this.start();
        return this;
    }

    /**
     * KafkaProperties.KAFKA_SERVER_URL
     */
    public Producer withServer(String server) {
        this.server = server;
        return this;
    }
    /**
     * KafkaProperties.KAFKA_SERVER_PORT
     */
    public Producer withPort(int server) {
        this.port = server;
        return this;
    }


    public void run() {
        int messageNo = 1;
        while (messageNo < maxMessages) {
            V messageStr = (V) (new Date().toString() + "Message_" + messageNo);
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<K, V>(topic,
                        (K) Integer.toString(messageNo),
                        messageStr), new DemoCallBack(startTime, Integer.toString(messageNo), messageStr));
            } else { // Send synchronously
                try {
                    sendMessage((K) Integer.toString(messageNo),  messageStr);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }

    public void sendMessage(K key, V value) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(topic,  key, value)).get();
        log("Sent message: (" + value + ")");
    }

    protected void log(String msg) {
        System.out.println(SimpleDateFormat.getInstance().format(new Date()) + " Producer:" + msg);
    }
}

class DemoCallBack<K, V> implements Callback {

    private final long startTime;
    private final K key;
    private final V message;

    public DemoCallBack(long startTime, K key, V message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
