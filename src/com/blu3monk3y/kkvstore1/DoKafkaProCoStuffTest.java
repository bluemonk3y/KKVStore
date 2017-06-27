package com.blu3monk3y.kkvstore1;

import com.blu3monk3y.kkvstore.util.Consumer;
import com.blu3monk3y.kkvstore.util.KafkaProperties;
import com.blu3monk3y.kkvstore.util.Producer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 *
 * Created by navery on 18/05/2017.
 *
 * TODO: look at leveraging embedded kafka/zkpr
 * https://gist.github.com/fjavieralba/7930018
 */
public class DoKafkaProCoStuffTest {

    private static final Logger log = LoggerFactory.getLogger(DoKafkaProCoStuffTest.class);

    @Test
    public void shouldDoProdToMultiGroupsTest() throws InterruptedException {
        String topic = "test-1-shouldDoProdToMultiGroupsTest";
        Consumer con1 = new Consumer(topic, "g1").withServer(KafkaProperties.KAFKA_SERVER_URL).withPort(KafkaProperties.KAFKA_SERVER_PORT).create();
        Consumer con2 = new Consumer(topic, "g2").withServer(KafkaProperties.KAFKA_SERVER_URL).withPort(KafkaProperties.KAFKA_SERVER_PORT).create();

        Producer pro = new Producer(topic, false, 100).
                withServer(KafkaProperties.KAFKA_SERVER_URL).
                withPort(KafkaProperties.KAFKA_SERVER_PORT).create();

        pro.start();

        int waited = 0;
        int pollPeriod = 100;
        while(con1.msgs() == 0 && (waited++ * pollPeriod)/1000 < 60 /** timeout -10-secs **/) {
            Thread.sleep(pollPeriod);
        }

        Thread.sleep(1000);

        System.out.println("Got:" + con1.msgs() + " ," + con2.msgs());
        assertThat("messages were received by both grps", con1.msgs() > 0 && con2.msgs() > 0);
        con1.shutdown();
        con2.shutdown();

    }
    @Test
    public void shouldDoProdCosTest() throws Exception {

        String topic = "test-123-SIMPLE";
        Consumer con = new Consumer(topic, "SIMPLE-g1").withServer(KafkaProperties.KAFKA_SERVER_URL).withPort(KafkaProperties.KAFKA_SERVER_PORT).create();
        Thread.sleep(10000);

        Producer pro = new Producer(topic, false, 100).
                withServer(KafkaProperties.KAFKA_SERVER_URL).
                withPort(KafkaProperties.KAFKA_SERVER_PORT).create();

        //pro.start();
        pro.sendMessage("Test-msg", "value");
        System.out.println("producer running =======================");

        int waited = 0;
        int pollPeriod = 100;
        while(con.msgs() == 0 && (waited++ * pollPeriod)/1000 < 20 /** timeout -10-secs **/) {
            Thread.sleep(pollPeriod);
        }

        Thread.sleep(1000);

        System.out.println("msgs consumed:" + con.msgs());



        con.shutdown();

        assertThat("messages were received", con.msgs(), greaterThan(0));
    }


}
