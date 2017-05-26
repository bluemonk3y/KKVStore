package com.blu3monk3y.kkvstore1;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Created by navery on 22/05/2017.
 */
public class SimpleObservableMapTest {

    @Test
    public void putGetTest() throws InterruptedException {

        SimpleObservableMap<String, String> map = new SimpleObservableMap<>("test-123", "g1");
        String value = new Date(). toString() + "value-1-putGetTest";

        System.out.println("NewMap:" + map.toString());
        map.put("key-1", value);
        String found = map.get("key-1", true);
        Assert.assertNotNull(found);
        Assert.assertEquals(value, found);
        System.out.println("Found:" + found);
        assertThat("messages were received", 100, greaterThan(0));
    }

    CountDownLatch receivedMsgs;
    @Test
    public void putObservableTest() throws InterruptedException {

        receivedMsgs = new CountDownLatch(1);

        SimpleObservableMap<String, String> map = new SimpleObservableMap<>("test-1", "g1");

        map.registerObserver(new SimpleObservableMap.Observer<String, String>() {
            @Override
            public boolean matches(String s) {
                return s.contains("key-");
            }

            @Override
            public String notify(String s, String s2) {
                System.out.println("TEST??? Notification received map:" + map.toString() + " v:" + s2);
                receivedMsgs.countDown();
                return null;
            }
        });

        String value = new Date(). toString() + "value-1-putObservableTest";
        map.put("key-1", value);

        boolean await = receivedMsgs.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(true, await);

        map.stop();
    }

}