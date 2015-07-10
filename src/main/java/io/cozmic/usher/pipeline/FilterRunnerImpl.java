package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
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

    public FilterRunnerImpl(String pluginName, FilterPlugin filterPlugin, MessageMatcher messageMatcher) {
        this.pluginName = pluginName;
        this.filterPlugin = filterPlugin;
        this.messageMatcher = messageMatcher;

    }

    @Override
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        filterPlugin.run(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain output plugin duplex stream for %s. Cause: %s", pluginName, cause.getMessage()), cause);
                run(messageStreamAsyncResultHandler);
                return;
            }
            final MessageStream messageStream = asyncResult.result();
            MessageStream filteredMessageStream = messageStream.filterBy(messageMatcher);

            messageStreamAsyncResultHandler.handle(Future.succeededFuture(filteredMessageStream));
        });
    }
}
