package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.ChannelFactory;
import io.cozmic.usher.core.CountDownFutureResult;
import io.cozmic.usher.core.InputRunner;
import io.cozmic.usher.plugins.PluginFactory;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.streams.ChannelFactoryImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created by chuck on 6/30/15.
 * <p>
 * Sets up data streams based on config file.... Example config:
 * <p>
 * inputs: [
 * {
 * type: TcpInput
 * },
 * {
 * type: HttpInput
 * }
 * ],
 * outputs: [
 * {
 * type: TcpOutput
 * }
 * ]
 */
public class PipelineVerticle extends AbstractVerticle {
    Logger logger = LoggerFactory.getLogger(PipelineVerticle.class.getName());

    public void start(final Future<Void> startedResult) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, IOException, UsherInitializationFailedException {

        final JsonObject config = config();
        final JsonObject usherConfig = config.getJsonObject("usher", new JsonObject());
        PluginFactory pluginFactory = new PluginFactory(getVertx(), config);
        List<InputRunner> inputRunners = pluginFactory.getInputRunners();

        final int inputCount = inputRunners.size();

        CountDownFutureResult<Void> dynamicStarter = CountDownFutureResult.dynamicStarter(inputCount);

        final OutputStreamMuxPool outputStreamMuxPool = new OutputStreamMuxPool(usherConfig, vertx, pluginFactory);
        final ChannelFactory channelFactory = new ChannelFactoryImpl(outputStreamMuxPool);


        for (InputRunner inputRunner : inputRunners) {
            inputRunner.start(startupResult -> {
                if (startupResult.failed()) {
                    final Throwable cause = startupResult.cause();
                    logger.error(cause.getMessage(), cause);
                    dynamicStarter.fail(cause);
                    return;
                }
                dynamicStarter.complete();
            }, channelFactory::createFullDuplexMuxChannel);
        }


        dynamicStarter.setHandler(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(cause.getMessage(), cause);
                startedResult.fail(cause);
                return;
            }

            startedResult.complete();
        });
    }


}
