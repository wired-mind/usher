package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by chuck on 6/30/15.
 */
public class OutputRunnerImpl implements OutputRunner {
    Logger logger = LoggerFactory.getLogger(OutputRunnerImpl.class.getName());
    private final String pluginName;
    private final JsonObject outputObj;
    private final OutputPlugin outputPlugin;
    private final MessageMatcher messageMatcher;
    private final InPipelineFactory inPipelineFactory;
    private final OutPipelineFactory outPipelineFactory;

    public OutputRunnerImpl(String pluginName, JsonObject outputObj, OutputPlugin outputPlugin, MessageMatcher messageMatcher, InPipelineFactory inPipelineFactory, OutPipelineFactory outPipelineFactory) {
        this.pluginName = pluginName;
        this.outputObj = outputObj;
        this.outputPlugin = outputPlugin;
        this.messageMatcher = messageMatcher;
        this.inPipelineFactory = inPipelineFactory;
        this.outPipelineFactory = outPipelineFactory;
    }

    @Override
    public void run(Handler<AsyncResult<MessageStream>> messageStreamAsyncResultHandler) {
        outputPlugin.run(duplexStreamAsyncResult -> {
            if (duplexStreamAsyncResult.failed()) {
                final Throwable cause = duplexStreamAsyncResult.cause();
                logger.error(String.format("Unable to obtain output plugin duplex stream for %s. Cause: %s", pluginName, cause.getMessage()), cause);
                run(messageStreamAsyncResultHandler);
                return;
            }
            final DuplexStream<Buffer, Buffer> duplexStream = duplexStreamAsyncResult.result();


            final OutPipeline outPipeline = outPipelineFactory.createDefaultOutPipeline(pluginName, outputObj, duplexStream.getWriteStream());
            final InPipeline inPipeline = inPipelineFactory.createDefaultInPipeline(pluginName, duplexStream);
            final MessageStream result = new MessageStream(inPipeline, outPipeline);
            result.setMessageMatcher(messageMatcher);
            messageStreamAsyncResultHandler.handle(Future.succeededFuture(result));

        });
    }

    @Override
    public void stop(MessageStream messageStream) {

        outputPlugin.stop(messageStream.getOutPipeline());
    }
}
