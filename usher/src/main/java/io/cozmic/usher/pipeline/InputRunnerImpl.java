package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

/**
 * Created by chuck on 6/25/15.
 */
public class InputRunnerImpl implements InputRunner {
    private final String pluginName;
    private final InputPlugin inputPlugin;
    private final MessageParserFactory inOutParserFactory;
    private final MessageFilterFactoryImpl outInFilterFactory;
    private final MessageMatcher messageMatcher;

    public InputRunnerImpl(String pluginName, InputPlugin inputPlugin, MessageMatcher messageMatcher, MessageParserFactory inOutParserFactory, MessageFilterFactoryImpl outInFilterFactory) {
        this.pluginName = pluginName;

        this.inputPlugin = inputPlugin;
        this.messageMatcher = messageMatcher;
        this.inOutParserFactory = inOutParserFactory;
        this.outInFilterFactory = outInFilterFactory;


    }

    @Override
    public void start(AsyncResultHandler<Void> startupHandler, Handler<MessageStream> messageStreamHandler) {

        inputPlugin.run(startupHandler::handle, duplexStream -> {
            MessageParser messageParser = inOutParserFactory.createParser(pluginName, duplexStream);
            final MessageFilter messageFilter = outInFilterFactory.createFilter(pluginName, messageMatcher, duplexStream.getWriteStream());
            messageStreamHandler.handle(new MessageStream(messageParser, messageFilter));
        });
    }
}
