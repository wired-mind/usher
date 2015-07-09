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
 * Created by chuck on 7/7/15.
 */
public class FilterRunnerImpl implements FilterRunner {
    Logger logger = LoggerFactory.getLogger(FilterRunnerImpl.class.getName());
    private final String pluginName;
    private final FilterPlugin filterPlugin;
    private final MessageMatcher messageMatcher;
    private final MessageParserFactoryImpl outInParserFactory;
    private final MessageFilterFactory inOutFilterFactory;

    public FilterRunnerImpl(String pluginName, FilterPlugin filterPlugin, MessageMatcher messageMatcher, MessageParserFactoryImpl outInParserFactory, MessageFilterFactory inOutFilterFactory) {
        this.pluginName = pluginName;
        this.filterPlugin = filterPlugin;
        this.messageMatcher = messageMatcher;
        this.outInParserFactory = outInParserFactory;
        this.inOutFilterFactory = inOutFilterFactory;

    }

    @Override
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        filterPlugin.run(duplexStreamAsyncResult -> {
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
}
