package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/30/15.
 */
public class OutputRunnerImpl implements OutputRunner {
    private final String pluginName;
    private final OutputPlugin outputPlugin;
    private final MessageParserFactoryImpl outInParserFactory;
    private final MessageFilterFactory inOutFilterFactory;

    public OutputRunnerImpl(String pluginName, OutputPlugin outputPlugin, MessageParserFactoryImpl outInParserFactory, MessageFilterFactory inOutFilterFactory) {
        this.pluginName = pluginName;
        this.outputPlugin = outputPlugin;
        this.outInParserFactory = outInParserFactory;
        this.inOutFilterFactory = inOutFilterFactory;
    }

    @Override
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
         outputPlugin.run(duplexStreamAsyncResult -> {
             if (duplexStreamAsyncResult.failed()) {
                 messageStreamAsyncResultHandler.handle(Future.failedFuture(duplexStreamAsyncResult.cause()));
                 return;
             }
             final DuplexStream<Buffer, Buffer> duplexStream = duplexStreamAsyncResult.result();
             final MessageFilter messageFilter = inOutFilterFactory.createFilter(pluginName, duplexStream.getWriteStream());
             final MessageParser messageParser = outInParserFactory.createParser(pluginName, duplexStream);
             messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(messageParser, messageFilter)));
         });
    }

    @Override
    public void stop(MessageFilter messageFilter) {
        outputPlugin.stop(messageFilter.getInnerWriteStream());
    }
}
