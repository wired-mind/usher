package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;



/**
 * Created by chuck on 7/7/15.
 */
public class FilterRunnerImpl implements FilterRunner {
    Logger logger = LoggerFactory.getLogger(FilterRunnerImpl.class.getName());
    private final String pluginName;
    private final JsonObject filterObj;
    private final FilterPlugin filterPlugin;
    private final MessageMatcher messageMatcher;
    private ErrorStrategy errorStrategy;

    public FilterRunnerImpl(String pluginName, JsonObject filterObj, FilterPlugin filterPlugin, MessageMatcher messageMatcher, ErrorStrategy errorStrategy) throws UsherInitializationFailedException {
        this.pluginName = pluginName;
        this.filterObj = filterObj;
        this.filterPlugin = filterPlugin;
        this.messageMatcher = messageMatcher;
        this.errorStrategy = errorStrategy;
    }

    @Override
    public void run(StreamMux mux, AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        final MessageInjectorImpl messageInjector = new MessageInjectorImpl(mux, messageMatcher);
        filterPlugin.run(messageInjector, asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain output plugin duplex stream for %s. Cause: %s", pluginName, cause.getMessage()), cause);
                run(mux, messageStreamAsyncResultHandler);
                return;
            }
            final MessageStream messageStream = asyncResult.result();
            messageStream.setMessageMatcher(messageMatcher);
            messageStream.setErrorStrategy(errorStrategy);
            messageStreamAsyncResultHandler.handle(Future.succeededFuture(messageStream));
        });
    }
}
