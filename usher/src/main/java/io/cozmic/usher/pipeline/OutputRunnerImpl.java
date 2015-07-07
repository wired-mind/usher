package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by chuck on 6/30/15.
 */
public class OutputRunnerImpl implements OutputRunner {
    Logger logger = LoggerFactory.getLogger(OutputRunnerImpl.class.getName());
    private final String pluginName;
    private final OutputPlugin outputPlugin;
    private final MessageMatcher messageMatcher;
    private final MessageParserFactoryImpl outInParserFactory;
    private final MessageFilterFactory inOutFilterFactory;

    public OutputRunnerImpl(String pluginName, OutputPlugin outputPlugin, MessageMatcher messageMatcher, MessageParserFactoryImpl outInParserFactory, MessageFilterFactory inOutFilterFactory) {
        this.pluginName = pluginName;
        this.outputPlugin = outputPlugin;
        this.messageMatcher = messageMatcher;
        this.outInParserFactory = outInParserFactory;
        this.inOutFilterFactory = inOutFilterFactory;
    }

    @Override
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
         outputPlugin.run(duplexStreamAsyncResult -> {
             if (duplexStreamAsyncResult.failed()) {
                 final Throwable cause = duplexStreamAsyncResult.cause();
                 logger.error(String.format("Unable to obtain output plugin duplex stream for %s. Cause: %s", pluginName, cause.getMessage()), cause);
                 run(messageStreamAsyncResultHandler);
                 return;
             }
             final DuplexStream<Buffer, Buffer> duplexStream = duplexStreamAsyncResult.result();
             final MessageFilter messageFilter = inOutFilterFactory.createFilter(pluginName, messageMatcher, duplexStream.getWriteStream());
             final MessageParser messageParser = outInParserFactory.createParser(pluginName, duplexStream);
             messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(messageParser, messageFilter)));
         });
    }

    @Override
    public void stop(MessageStream messageStream) {
        outputPlugin.stop(messageStream.getMessageFilter().getInnerWriteStream());
    }
}
