package com.blu3monk3y.kkvstore.util;

/**
 * Created by navery on 18/05/2017.
 */



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.*;
import java.util.concurrent.*;

public abstract class ShutdownableThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ShutdownableThread.class);

    private final boolean isInterruptible;
    //    extends Thread(name) with Logging {
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    protected void doWork() {
    };

    public ShutdownableThread(String name, boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;
        setDaemon(false);
        info("[" + name + "]: ");

        shutdownLatch.countDown();
    }

    public void shutdown() {
        try {
            initiateShutdown();
            awaitShutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected abstract void shutdownClientCode();


    public void run() {
        info("ShutdownableThread Starting");
        try {
            while (isRunning.get())
                doWork();
            // clean exit
            info("cleaning up");
            shutdownClientCode();
        } catch (Error e) {
            isRunning.set(false);
            shutdownLatch.countDown();
            System.out.println("Stopped");
            System.exit(-1);
        } catch( Throwable e) {
            if (e instanceof  InterruptedException) {
             // nada
            } else {
                e.printStackTrace();
                if (isRunning.get()) {
                    System.out.println("Error due to" + e);
                }
            }
        }
    }
    public Boolean initiateShutdown() {
        if (isRunning.compareAndSet(true, false)) {
            System.out.println("Shutting down");
            if (isInterruptible())
                interrupt();
            return true;
        } else
            return false;
    };

    /**
         * After calling initiateShutdown(), use this API to wait until the shutdown is complete
         */

    public void waitShutdown() throws InterruptedException {
        System.out.println("Shutdown completed");
        shutdownLatch.await();

    }
    protected void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
    protected boolean isInterruptible(){
        return isInterruptible;
    };

    protected void info(String msg) {
        log.info(msg);
    }

}
