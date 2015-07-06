package io.cozmic.usher.streams;

import io.cozmic.usher.core.Channel;
import io.cozmic.usher.core.MessageFilter;
import io.cozmic.usher.core.MessageParser;
import io.vertx.core.Handler;
import io.vertx.core.streams.Pump;

/**
 * Created by chuck on 6/30/15.
 */
public class ChannelImpl implements Channel {

    private final MessageStream inMessageStream;
    private final MessageStream outMessageStream;
    private Pump inToOutPump;
    private Pump outToInPump;
    private Handler<Void> endHandler;

    public ChannelImpl(MessageStream inputMessageStream, MessageStream outputMessageStream) {
        this.inMessageStream = inputMessageStream;
        this.outMessageStream = outputMessageStream;
        inToOutPump = createPump(inputMessageStream, outputMessageStream);
        outToInPump = createPump(outputMessageStream, inputMessageStream);

        inputMessageStream.getReadStream().endHandler(v -> {
            inToOutPump.stop();
            outToInPump.stop();
            if (endHandler != null) endHandler.handle(null);
        });
    }

    public Pump createPump(MessageStream inMessageStream, MessageStream outMessageStream) {
        final MessageParser messageParser = inMessageStream.getMessageParser();
        final MessageFilter messageFilter = outMessageStream.getMessageFilter();
        return Pump.pump(messageParser, messageFilter);
    }

    @Override
    public Channel start() {
        inToOutPump.start();
        outToInPump.start();

        inMessageStream.resume();
        return this;
    }

    @Override
    public Channel endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }
}
