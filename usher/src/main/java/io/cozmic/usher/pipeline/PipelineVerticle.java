package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.CountDownFutureResult;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.List;

/**
 * Created by chuck on 6/30/15.
 *
 * Sets up data streams based on config file.... Example config:
 *
 * inputs: [
 *    {
 *      type: TcpInput
 *    },
 *    {
 *      type: HttpInput
 *    }
 * ],
 * outputs: [
 *    {
 *        type: TcpOutput
 *    }
 * ]
 */
public class PipelineVerticle extends AbstractVerticle {
    Logger logger = LoggerFactory.getLogger(PipelineVerticle.class.getName());
    public void start(final Future<Void> startedResult) {

        PluginFactory pluginFactory = new PluginFactory(getVertx());
        List<InputRunner> inputRunners = pluginFactory.createInputRunners(config().getJsonArray("inputs"));
        List<OutputRunner> outputRunners = pluginFactory.createOutputRunners(config().getJsonArray("outputs"));

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
            }, inputMessageParsingStream -> {
                for (OutputRunner outputRunner : outputRunners) {
                    outputRunner.run(asyncResult -> {
                        if (asyncResult.failed()) {
                            final Throwable cause = asyncResult.cause();
                            logger.error(cause.getMessage(), cause);
                            dynamicStarter.fail(cause);
                            return;
                        }

                        final MessageFilteringStream messageFilteringStream = asyncResult.result();
                        final MessageParser messageParser = inputMessageParsingStream.getMessageParser();
                        final WriteStream<Buffer> inputWriteStream = inputMessageParsingStream.getWriteStream();
                        final MessageFilter messageFilter = messageFilteringStream.getMessageFilter();
                        final ReadStream<Buffer> outputReadStream = messageFilteringStream.getReadStream();

                        final Pump inToOutPump = Pump.pump(messageParser, messageFilter);
                        final Pump outToInPump = Pump.pump(outputReadStream, inputWriteStream);
                        inToOutPump.start();
                        outToInPump.start();
                        messageParser.endHandler(v -> {
                            outputRunner.stop(messageFilter);
                            inToOutPump.stop();
                            outToInPump.stop();
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
