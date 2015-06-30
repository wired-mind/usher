package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.OutputPlugin;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/30/15.
 */
public class OutputRunnerImpl implements OutputRunner {
    private final OutputPlugin outputPlugin;
    private final MessageFilterFactory messageFilterFactory;

    public OutputRunnerImpl(OutputPlugin outputPlugin, MessageFilterFactory messageFilterFactory) {
        this.outputPlugin = outputPlugin;
        this.messageFilterFactory = messageFilterFactory;
    }

    @Override
    public void run(AsyncResultHandler<MessageFilteringStream> messageFilteringStreamAsyncResultHandler) {
         outputPlugin.run(duplexStreamAsyncResult -> {
             if (duplexStreamAsyncResult.failed()) {
                 messageFilteringStreamAsyncResultHandler.handle(Future.failedFuture(duplexStreamAsyncResult.cause()));
                 return;
             }
             final DuplexStream<Buffer, Buffer> duplexStream = duplexStreamAsyncResult.result();
             final MessageFilter messageWriteStream = messageFilterFactory.createFilter(duplexStream.getWriteStream());
             messageFilteringStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageFilteringStream(duplexStream.getReadStream(), messageWriteStream)));
         });
    }

    @Override
    public void stop(MessageFilter messageFilter) {
        outputPlugin.stop(messageFilter.getInnerWriteStream());
    }
}
