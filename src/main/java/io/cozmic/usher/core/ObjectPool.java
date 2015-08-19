package io.cozmic.usher.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Inspired by easy-pool (https://github.com/ova2/easy-pool) but adapted for vertx
 *
 * @param <T>
 */
public abstract class ObjectPool<T> {
    private final int minIdle;
    Logger logger = LoggerFactory.getLogger(className());
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
        this.minIdle = minIdle;
        this.configObj = new JsonObject();
        // initialize pool
        initialize(minIdle);
    }

    /**
     * Creates the pool.
     *
     * Use minIdle = -1 to prevent the pool from sharing objects. Essentially it disables the pooling behavior.
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
        minIdle = configObj.getInteger("minIdle", 3);
        final Integer maxIdle = configObj.getInteger("maxIdle", 100);
        final Integer validationInterval = configObj.getInteger("validationInterval", 60);

        logger.info(String.format("Creating %s pool. MinIdle: %d", className(), minIdle));
        // initialize pool
        vertx.runOnContext(v -> {
            initialize(minIdle);
        });

        // check pool conditions in a separate thread
        timerId = vertx.setPeriodic(validationInterval * 1000, timeoutId -> {
            int size = pool.size();
            int sizeToBeRemoved = size;

            logger.info(String.format("Removing all %d objects from %s", sizeToBeRemoved, className()));
            // First, let's remove all the objects from the pool (reset to zero)
            for (int i = 0; i < sizeToBeRemoved; i++) {
                final T obj = doRemoveFromPool();
                if (obj != null) {
                    destroyObject(obj);
                }
            }

            // Then, let's recreate the minIdle count. (This way we don't continue to reuse an object for ever. We slowly replace them.)

            int sizeToBeAdded = minIdle;
            logger.info(String.format("Recreating %d objects in %s", sizeToBeAdded, className()));
            for (int i = 0; i < sizeToBeAdded; i++) {
                createObject(asyncResult -> {
                    if (asyncResult.succeeded()) {
                        pool.add(asyncResult.result());
                        poolSize.inc();
                    }
                });
            }

        });
    }

    /**
     * Gets the next free object from the pool. If the pool doesn't contain any objects,
     * a new object will be created and given to the caller of this method back.
     *
     * @return T borrowed object
     */
    public void borrowObject(Handler<AsyncResult<T>> readyHandler) {
        T object;
        if ((object = doRemoveFromPool()) == null) {
            createObject(asyncResult -> {
                if (asyncResult.failed()) {
                    readyHandler.handle(Future.failedFuture(asyncResult.cause()));
                    return;
                }
                logger.info(String.format("Tried to borrow object from %s. Miss. Created new object.", className()));
                poolMisses.inc();
                readyHandler.handle(Future.succeededFuture(asyncResult.result()));
            });
            return;
        }

        logger.info(String.format("Borrowing object from %s", className()));
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

        final boolean poolDisabled = minIdle == -1;
        if (poolDisabled) {
            logger.info(String.format("Returning object to %s. Pool is disabled. Destroying object.", className()));
            destroyObject(object);
            return;
        }

        logger.info(String.format("Returning object to %s", className()));
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

    public int getPoolSize() {
        return pool.size();
    }
}