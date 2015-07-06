package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.ChannelImpl;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.impl.EventBusImpl;
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

    public void start(final Future<Void> startedResult) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, IOException {

        PluginFactory pluginFactory = new PluginFactory(getVertx(), config());
        List<InputRunner> inputRunners = pluginFactory.getInputRunners();
        List<OutputRunner> outputRunners = pluginFactory.getOutputRunners();

        CountDownFutureResult<Void> dynamicStarter = CountDownFutureResult.dynamicStarter(inputRunners.size(), startedResult);

        for (InputRunner inputRunner : inputRunners) {

            inputRunner.start(startupResult -> {
                if (startupResult.failed()) {
                    final Throwable cause = startupResult.cause();
                    logger.error(cause.getMessage(), cause);
                    dynamicStarter.fail(cause);
                    return;
                }
                dynamicStarter.complete();
            }, inputMessageStream -> {

                //Pause the in stream until the out stream is ready
                inputMessageStream.pause();
                final Router router = new DemultiplexingRouter(outputRunners);
                for (OutputRunner outputRunner : outputRunners) {
                    outputRunner.run(asyncResult -> {
                        if (asyncResult.failed()) {
                            final Throwable cause = asyncResult.cause();
                            logger.error(cause.getMessage(), cause);
                            dynamicStarter.fail(cause);
                            return;
                        }


                        final MessageStream outputMessageStream = asyncResult.result();
                        Channel channel = new ChannelImpl(inputMessageStream, outputMessageStream);
                        channel.start().endHandler(v -> {
                            outputRunner.stop(outputMessageStream.getMessageFilter());
                        });
                    });
                }


            });
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
