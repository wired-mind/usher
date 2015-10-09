package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/7/15.
 */
public class RetryErrorStrategy implements ErrorStrategy {
    static Logger logger = LoggerFactory.getLogger(RetryErrorStrategy.class.getName());
    private final Vertx vertx;
    private final JsonObject config;
    private final Integer maxDelayMillis;
    private final Boolean exponentialBackoff;
    private Integer maxRetries;
    private Integer retryDelayMillis;
    private Double retryDelayMultiplier;

    public RetryErrorStrategy(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        maxRetries = config.getInteger("maxRetries", -1);
        maxDelayMillis = config.getInteger("maxDelayMillis", 10_000);
        exponentialBackoff = config.getBoolean("exponentialBackoff", false);
        retryDelayMillis = config.getInteger("retryDelayMillis", 1_000);
        retryDelayMultiplier = config.getDouble("retryDelayMultiplier", 1.5);
    }

    @Override
    public OutPipeline wrap(OutPipeline outPipeline) {
        return new OutPipeline() {


            private Handler<AsyncResult<Void>> writeCompleteHandler;

            @Override
            public void stop(WriteStreamPool pool) {
                outPipeline.stop(pool);
            }

            @Override
            public OutPipeline writeCompleteHandler(Handler<AsyncResult<Void>> handler) {
                writeCompleteHandler = handler;
                return this;
            }

            @Override
            public void close() {
                outPipeline.close();
            }

            @Override
            public AsyncWriteStream<PipelinePack> write(PipelinePack data, Handler<AsyncResult<Void>> writeCompleteHandler) {
                throw new UnsupportedOperationException();
            }

            @Override
            public WriteStream<PipelinePack> exceptionHandler(Handler<Throwable> handler) {
                return this;
            }

            @Override
            public WriteStream<PipelinePack> write(PipelinePack data) {
                RetryHandler retryHandler = new RetryHandler(vertx);
                retryHandler = retryHandler
                        .retryOn(Throwable.class)
                        .withUniformJitter();    // add between +/- 100 ms randomly;
                if (maxRetries == -1) {
                    retryHandler = retryHandler.retryInfinitely();
                } else {
                    retryHandler = retryHandler.withMaxRetries(maxRetries);
                }

                if (exponentialBackoff) {
                    retryHandler = retryHandler.withExponentialBackoff(retryDelayMillis, retryDelayMultiplier)
                            .withMaxDelay(maxDelayMillis);
                } else {
                    retryHandler = retryHandler.withFixedBackoff(retryDelayMillis);
                }

                retryHandler.runWithRetry(context -> {
                    final Throwable lastThrowable = context.getLastThrowable();
                    logger.warn("Retry attempt: " + context.getRetryCount(), lastThrowable);


                    data.setRetryContext(context);
                    outPipeline.write(data, context);
                }, writeCompleteHandler::handle);




                return this;
            }

            @Override
            public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
                outPipeline.setWriteQueueMaxSize(maxSize);
                return this;
            }

            @Override
            public boolean writeQueueFull() {
                return outPipeline.writeQueueFull();
            }

            @Override
            public WriteStream<PipelinePack> drainHandler(Handler<Void> handler) {
                outPipeline.drainHandler(handler);
                return this;
            }
        };
    }
}
