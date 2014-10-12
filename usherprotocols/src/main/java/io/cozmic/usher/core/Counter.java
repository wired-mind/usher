package io.cozmic.usher.core;

import org.vertx.java.core.shareddata.Shareable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by chuck on 10/2/14.
 */
public class Counter implements Shareable {
    private final String name;
    public AtomicInteger MyCounter = new AtomicInteger(0);

    public Counter() {
        this("default");
    }

    public Counter(String name) {

        this.name = name;
    }

    public String getName() {
        return name;
    }
}
