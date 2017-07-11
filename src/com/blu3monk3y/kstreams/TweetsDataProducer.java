package com.blu3monk3y.kstreams;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.*;

/**
 */
public class TweetsDataProducer {

    long startTime = System.currentTimeMillis();
    final KafkaProducer<String, GenericRow> producer;

    public TweetsDataProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProductStreamProducers");



        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        Schema schema = schemaBuilder
                .field("tweettime", Schema.INT64_SCHEMA)
                .field("user", Schema.STRING_SCHEMA)
                .field("msg", Schema.STRING_SCHEMA);


        Serializer<GenericRow> kqlJsonPOJOSerializer = new KsqlJsonSerializer(schema);
        this.producer = new KafkaProducer<String, GenericRow>(props, new StringSerializer(), (Serializer<GenericRow>) kqlJsonPOJOSerializer);
    }

    public void genericRowOrdersStreamMap(String topicName) {
        long maxInterval = 10;
        int messageCount = 100;
        String[] usernames = new String[] { "jason_b", "benny_black", "z_nation", "crazy_koko"};
        String[] msgs = new String[] { "ksql is a game change #kafka", "confluent change kafka value prop", "another streaming sql competitor",
                                        "is kafka changing? #kafka", "confluent cloud looks interesting", "enjoying the show #confluentsf", "impressive #conflient"};

        long currentTime = System.currentTimeMillis() - 60 * 60 * 1000;

        for(int i = 0; i < messageCount; i++) {

            List<Object> columns = new ArrayList();
            currentTime = (long)(1000*Math.random()) + currentTime;
            // tweettime
            columns.add(Long.valueOf(currentTime));
            columns.add(usernames[i % usernames.length]);
            columns.add(msgs[i % msgs.length]);

            GenericRow genericRow = new GenericRow(columns);

            ProducerRecord producerRecord = new ProducerRecord(topicName, String.valueOf(i+1), genericRow);

            producer.send(producerRecord);
            System.out.println((i+1)+" --> ("+genericRow+")");

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        new TweetsDataProducer().genericRowOrdersStreamMap("tweets_topic_json");
    }
}