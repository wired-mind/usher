package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.InputPlugin;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

/**
 * Created by chuck on 6/25/15.
 */
public class InputRunnerImpl implements InputRunner {
    private final InputPlugin inputPlugin;
    private final MessageParserFactory messageParserFactory;


    public InputRunnerImpl(InputPlugin inputPlugin, MessageParserFactory messageParserFactory) {

        this.inputPlugin = inputPlugin;
        this.messageParserFactory = messageParserFactory;
    }

    @Override
    public void start(AsyncResultHandler<Void> startupHandler, Handler<MessageParsingStream> messageParsingStreamHandler) {

        inputPlugin.run(startupHandler::handle, duplexStream -> {
            MessageParser messageParser = messageParserFactory.createParser(duplexStream.getReadStream());
            messageParsingStreamHandler.handle(new MessageParsingStream(messageParser, duplexStream.getWriteStream()));
        });
    }
}
