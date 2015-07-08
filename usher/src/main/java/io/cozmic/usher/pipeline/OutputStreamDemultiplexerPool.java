package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.PluginFactory;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

/**
 * Created by chuck on 7/6/15.
 */
public class OutputStreamDemultiplexerPool extends ObjectPool<StreamMux> {
    Logger logger = LoggerFactory.getLogger(OutputStreamDemultiplexerPool.class.getName());

    private List<OutputRunner> outputRunners;
    private List<FilterRunner> filterRunners;

    public OutputStreamDemultiplexerPool(JsonObject configObj, Vertx vertx, PluginFactory pluginFactory) {
        super(configObj, vertx);

        outputRunners = pluginFactory.getOutputRunners();
        filterRunners = pluginFactory.getFilterRunners();
    }

    @Override
    protected Class className() {
        return OutputStreamDemultiplexerPool.class;
    }

    @Override
    protected void destroyObject(StreamMux obj) {
        obj.unregisterAllConsumers();
    }

    @Override
    protected void createObject(AsyncResultHandler<StreamMux> readyHandler) {


        final int runnerCount = outputRunners.size() + filterRunners.size();
        CountDownFutureResult<Void> dynamicStarter = CountDownFutureResult.dynamicStarter(runnerCount);

        final StreamMux streamMux = new StreamMuxImpl(vertx);

        for (OutputRunner outputRunner : outputRunners) {
            outputRunner.run(asyncResult -> {
                if (asyncResult.failed()) {
                    final Throwable cause = asyncResult.cause();
                    logger.error(cause.getMessage(), cause);
                    dynamicStarter.fail(cause);
                }

                final MessageStream outputMessageStream = asyncResult.result();

                final MuxRegistration muxRegistration = streamMux.addStream(outputMessageStream, true);
                muxRegistration.endHandler(v -> outputRunner.stop(outputMessageStream));
                dynamicStarter.complete();

            });
        }

//        for (FilterRunner filterRunner : filterRunners) {
//            filterRunner.run(asyncResult -> {
//
//            });
//        }

        dynamicStarter.setHandler(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(cause.getMessage(), cause);
                logger.error(String.format("Unable to obtain streamMux from pool. Trying again. %s", cause.getMessage()), cause);
                createObject(readyHandler);
                return;
            }

            readyHandler.handle(Future.succeededFuture(streamMux));
        });


    }
}
