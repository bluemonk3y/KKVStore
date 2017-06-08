package com.blu3monk3y.kkvstore1;

import com.blu3monk3y.kkvstore.util.Consumer;
import com.blu3monk3y.kkvstore.util.KafkaProperties;
import com.blu3monk3y.kkvstore.util.Producer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by navery on 19/05/2017.
 */
public class SimpleObservableMap<K, V>  {

    /**
     * Hooks into kafka pro-co for persistence
     */
    final Consumer<K,V> con;
    final Producer<K,V> pro;
    final Map map = new HashMap<K, V>();

    /**
     * TODO:
     * sort out expiration (i.e. ttl), invalidation (i.e. #errors) -
     * they are not persisted and should be journaled
     */
    final Map observers = new ConcurrentHashMap<K,V>();

    public void registerObserver(Observer<V, V> observer) {
        observers.put(observer.toString(), observer);
    }

    public void stop() {
        con.shutdown();
    }

    public static interface Observer<K, V> {
        boolean matches(K k);
        V notify(K k, V v);
    }

    public SimpleObservableMap(String topic, String consumerGroup, String server, int port){
        con = new Consumer<K, V>(topic, consumerGroup) {
            @Override
            public void handle(K key, V value, long offset) {

                map.put(key, value);
                // err - iterate observers - barf
                for (Iterator<Observer<K,V>> iterator = observers.values().iterator(); iterator.hasNext(); ) {
                    Observer<K, V> next = iterator.next();
                    if (next.matches(key)) {
                        log("NOTIFY KAFKA:" + key + ":"+ value);
                        next.notify(key, value);
                    }
                }
            }
        }.withServer(server).withPort(port).create();
        pro = new Producer(topic, false, 100).withServer(server).withPort(port).create();
    }


    /**
     * Put into map via kafka
     * @param key
     * @param value
     */

    public void put(K key, V value) {
        try {
            pro.sendMessage(key, value);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Its a bit crap - due to latency - will optionally try to wait until we have something...
     * @param key
     * @param wait
     * @return
     */
    public V get(K key, boolean wait) throws InterruptedException {
        V value = (V) map.get(key);
        if (value != null) return  value;

        // Wait for something to arrive
        int waitCount = 0;
        int waitLimit = 20;
        long waitSleep = 100;
        while (value == null && waitCount++ * waitSleep < waitLimit * 1000) {
            value = (V) map.get(key);
            Thread.sleep(waitSleep);
        }
        return value;
    }

}
