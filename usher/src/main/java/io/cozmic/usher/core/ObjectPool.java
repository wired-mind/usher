package io.cozmic.usher.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Inspired by easy-pool (https://github.com/ova2/easy-pool) but adapted for vertx
 * @param <T>
 */
public abstract class ObjectPool<T>
{
    protected final JsonObject configObj;
    protected Vertx vertx;
    private ConcurrentLinkedQueue<T> pool;
    private long timerId;
    private MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate("usher");
    private final Counter poolSize = metricRegistry.counter(name(className(), "pool-size"));
    private final Counter poolMisses = metricRegistry.counter(name(className(), "pool-misses"));

    protected abstract Class className();


    /**
     * Creates the pool.
     *
     * @param minIdle minimum number of objects residing in the pool
     */
    public ObjectPool(final int minIdle) {
        this.configObj = new JsonObject();
        // initialize pool
        initialize(minIdle);
    }

    /**
     * Creates the pool.
     *
     * @param minIdle            minimum number of objects residing in the pool
     * @param maxIdle            maximum number of objects residing in the pool
     * @param validationInterval time in seconds for periodical checking of minIdle / maxIdle conditions in a separate thread.
     *                           When the number of objects is less than minIdle, missing instances will be created.
     *                           When the number of objects is greater than maxIdle, too many instances will be removed.
     */

    public ObjectPool(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        final Integer minIdle = configObj.getInteger("minIdle", 3);
        final Integer maxIdle = configObj.getInteger("maxIdle", 100);
        final Integer validationInterval = configObj.getInteger("validationInterval", 5);
        // initialize pool
        initialize(minIdle);

        // check pool conditions in a separate thread
        timerId = vertx.setPeriodic(validationInterval * 1000, timeoutId -> {
            int size = pool.size();
            if (size < minIdle) {
                int sizeToBeAdded = minIdle - size;
                for (int i = 0; i < sizeToBeAdded; i++) {
                    createObject(asyncResult -> {
                        if (asyncResult.succeeded()) {
                            pool.add(asyncResult.result());
                            poolSize.inc();
                        }
                    });
                }
            } else if (size > maxIdle) {
                int sizeToBeRemoved = size - maxIdle;
                for (int i = 0; i < sizeToBeRemoved; i++) {
                    final T obj = doRemoveFromPool();
                    if (obj != null) {
                        destroyObject(obj);
                    }
                }
            }
        });
    }

    /**
     * Gets the next free object from the pool. If the pool doesn't contain any objects,
     * a new object will be created and given to the caller of this method back.
     *
     * @return T borrowed object
     */
    public void borrowObject(AsyncResultHandler<T> readyHandler) {
        T object;
        if ((object = doRemoveFromPool()) == null) {
            createObject(asyncResult -> {
                if (asyncResult.failed()) {
                    readyHandler.handle(Future.failedFuture(asyncResult.cause()));
                    return;
                }
                poolMisses.inc();
                readyHandler.handle(Future.succeededFuture(asyncResult.result()));
            });
            return;
        }

        readyHandler.handle(Future.succeededFuture(object));
    }

    private T doRemoveFromPool() {
        final T object = pool.poll();
        if (object != null) poolSize.dec();
        return object;
    }

    /**
     * Returns object back to the pool.
     *
     * @param object object to be returned
     */
    public void returnObject(T object) {
        if (object == null) {
            return;
        }

        this.pool.offer(object);
        poolSize.inc();
    }

    /**
     * Shutdown this pool.
     */
    public void shutdown() {
        if (vertx != null) vertx.cancelTimer(timerId);
    }

    protected abstract void destroyObject(T obj);

    /**
     * Creates a new object asynchronously
     *
     * @return T new object
     */
    protected abstract void createObject(AsyncResultHandler<T> readyHandler);

    protected void initialize(final int minIdle) {
        pool = new ConcurrentLinkedQueue<T>();

        for (int i = 0; i < minIdle; i++) {
            createObject(asyncResult -> {
                if (asyncResult.succeeded()) {
                    pool.add(asyncResult.result());
                    poolSize.inc();
                }
            });
        }
    }
}