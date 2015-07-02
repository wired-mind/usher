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

    public ChannelImpl(MessageStream inMessageStream, MessageStream outMessageStream) {
        this.inMessageStream = inMessageStream;
        this.outMessageStream = outMessageStream;
        inToOutPump = createPump(inMessageStream, outMessageStream);
        outToInPump = createPump(outMessageStream, inMessageStream);

        inMessageStream.getReadStream().endHandler(v -> {
            inToOutPump.stop();
            outToInPump.stop();
            if (endHandler != null) endHandler.handle(null);
        });
    }

    public Pump createPump(MessageStream inMessageStream, MessageStream outMessageStream) {
        final MessageParser inOutMessageParser = inMessageStream.getMessageParser();
        final MessageFilter inOutmessageFilter = outMessageStream.getMessageFilter();
        return Pump.pump(inOutMessageParser, inOutmessageFilter);
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
