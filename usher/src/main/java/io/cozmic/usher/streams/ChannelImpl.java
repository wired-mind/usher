package io.cozmic.usher.streams;

import io.cozmic.usher.core.*;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;

import java.util.List;

/**
 * Created by chuck on 6/30/15.
 */
public class ChannelImpl implements Channel {
    Logger logger = LoggerFactory.getLogger(ChannelImpl.class.getName());

    private final MessageParser messageParser;
    private final ObjectPool<StreamDemultiplexer> outStreamDemuxPool;
    private Pump inToOutPump;
    private Handler<Void> endHandler;
    private List<MessageParser> responseStreams;
    private StreamDemultiplexer outStreamDemultiplexer;
    private MessageConsumer optionalMessageConsumer;


    public ChannelImpl(MessageParser messageParser, ObjectPool<StreamDemultiplexer> outStreamDemuxPool ) {
        this.messageParser = messageParser;
        this.outStreamDemuxPool = outStreamDemuxPool;
        //Pause the in stream until the out stream is ready
        messageParser.pause();
    }




    @Override
    public Channel start() {
        inToOutPump.start();

        messageParser.resume();
        return this;
    }

    @Override
    public Channel stop() {
        doStop();
        return this;
    }

    @Override
    public Channel endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public List<MessageParser> getResponseStreams() {

        return responseStreams;
    }

    @Override
    public void init(AsyncResultHandler<Channel> readyHandler) {
        init(null, readyHandler);
    }

    @Override
    public void init(MessageStream optionalMessageStream, AsyncResultHandler<Channel> readyHandler) {
        outStreamDemuxPool.borrowObject(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error("Unable to obtain outStreamDemux. Retrying: " + cause.getMessage(), cause);

                init(optionalMessageStream, readyHandler);
                return;
            }
            outStreamDemultiplexer = asyncResult.result();

            if (optionalMessageStream != null) {
                optionalMessageConsumer = outStreamDemultiplexer.consumer(optionalMessageStream, false);
            }

            responseStreams = outStreamDemultiplexer.getResponseStreams();

            inToOutPump = Pump.pump(messageParser, outStreamDemultiplexer);

            messageParser.endHandler(v -> {
                if (endHandler != null) endHandler.handle(null);
                doStop();
            });

            readyHandler.handle(Future.succeededFuture(this));
        });
    }

    private void doStop() {
        if (inToOutPump != null) inToOutPump.stop();
        if (optionalMessageConsumer != null) optionalMessageConsumer.unregister();
        if (outStreamDemuxPool != null) outStreamDemuxPool.returnObject(outStreamDemultiplexer);
    }

}
