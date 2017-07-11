package com.blu3monk3y.kstreams;

import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class DataProducer {

    long startTime = System.currentTimeMillis();
    final KafkaProducer<String, GenericRow> producer;

    public DataProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProductStreamProducers");



        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        Schema schema = schemaBuilder
                .field("ordertime", Schema.INT64_SCHEMA)
                .field("orderid", Schema.INT64_SCHEMA)
                .field("itemid", Schema.STRING_SCHEMA)
                .field("orderunits", Schema.FLOAT64_SCHEMA)
                .field("arraycol",schemaBuilder.array(Schema.FLOAT64_SCHEMA))
                .field("mapcol", schemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));


        Serializer<GenericRow> kqlJsonPOJOSerializer = new KsqlJsonSerializer(schema);
        this.producer = new KafkaProducer<String, GenericRow>(props, new StringSerializer(), (Serializer<GenericRow>) kqlJsonPOJOSerializer);
    }

    public void genericRowOrdersStreamMap(String orderKafkaTopicName) {
        long maxInterval = 10;
        int messageCount = 1000;

        for(int i = 0; i < messageCount; i++) {
            long currentTime = System.currentTimeMillis();
            List<Object> columns = new ArrayList();
            currentTime = (long)(1000*Math.random()) + currentTime;
            // ordertime
            columns.add(Long.valueOf(currentTime));

            //orderid
            columns.add(String.valueOf(i+1));
            //itemid
            int productId = (int)(10*Math.random());
            columns.add("Item_"+productId);

            //units
            columns.add((double)((int)(10*Math.random())));

            Double[] prices = new Double[]{(10*Math.random()), (10*Math.random()), (10*Math.random()),
                    (10*Math.random()), (10*Math.random())};

            columns.add(prices);

            Map<String, Double> map = new HashMap<>();
            map.put("key1", (10*Math.random()));
            map.put("key2", (10*Math.random()));
            map.put("key3", (10*Math.random()));

            columns.add(map);

            GenericRow genericRow = new GenericRow(columns);

            ProducerRecord
                    producerRecord = new ProducerRecord(orderKafkaTopicName, String.valueOf(i+1), genericRow);

            producer.send(producerRecord);
            System.out.println((i+1)+" --> ("+genericRow+")");

            try {
                Thread.sleep((long)(maxInterval*Math.random()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        new DataProducer().genericRowOrdersStreamMap("orders_topic_json");
    }
}