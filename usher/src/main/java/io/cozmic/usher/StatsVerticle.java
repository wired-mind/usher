package io.cozmic.usher;

import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

/**
 * Created by chuck on 10/3/14.
 */
public class StatsVerticle extends Verticle {
    private long last;

    private long[] proxyStats = new long[]{0,0};
    private long start;
    private long totProxyReceived;
    public void start(final Future<Void> startedResult) {
        vertx.eventBus().registerHandler("stats:proxyReceive", new Handler<Message<Integer>>() {
            @Override
            public void handle(Message<Integer> msg) {
                if (last == 0) {
                    last = start = System.currentTimeMillis();
                }
                proxyStats[0] += msg.body();
                proxyStats[1] += msg.body();
            }
        });
        vertx.setPeriodic(3000, new Handler<Long>() {
            public void handle(Long id) {
                if (last != 0) {
                    calculateAndDisplayStats(proxyStats, "proxyReceive");
                }
            }
        });
    }

    protected void calculateAndDisplayStats(long[] stats, String name) {
        long now = System.currentTimeMillis();
        double rate = 1000 * (double) stats[0] / (now - last);
        double avRate = 1000 * (double) stats[1] / (now - start);
        stats[0] = 0;
        System.out.println("[" + name + "]" + (now - start) + " Rate: count/sec: " + rate + " Average rate: " + avRate);
        last = now;
    }
}
