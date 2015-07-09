package io.cozmic.usher.streams;

import io.cozmic.usher.core.*;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;

import java.util.Objects;

/**
 * Created by chuck on 7/6/15.
 */
public class ChannelFactoryImpl implements ChannelFactory {
    Logger logger = LoggerFactory.getLogger(ChannelFactoryImpl.class.getName());
    private final ObjectPool<StreamMux> outStreamMuxPool;

    public ChannelFactoryImpl(ObjectPool<StreamMux> outStreamMuxPool) {
        this.outStreamMuxPool = outStreamMuxPool;
    }

    @Override
    public void createFullDuplexMuxChannel(MessageStream inputStream) {
        outStreamMuxPool.borrowObject(asyncResult -> {
            StreamMux outStreamMux = asyncResult.result();
            ensureStreamMux(outStreamMux);

            Channel channel = new FullDuplexMuxChannel(inputStream, outStreamMux);
            channel.start();
        });
    }

    private void ensureStreamMux(StreamMux streamMux) {
        Objects.requireNonNull(streamMux, "OutStreamMuxPool should never return with errors. It will keep trying to fully initialize a mux before returning, never returning an error. If you see this error, something changed.");
    }

    /**
     * Created by chuck on 6/30/15.
     */
    public class FullDuplexMuxChannel implements Channel {
        Logger logger = LoggerFactory.getLogger(FullDuplexMuxChannel.class.getName());

        private final MessageStream messageStream;

        private Pump inToOutPump;
        private Pump outToInPump;
        private Handler<Void> endHandler;
        private StreamMux outStreamMux;


        public FullDuplexMuxChannel(MessageStream messageStream, StreamMux outStreamMux) {
            this.messageStream = messageStream;
            //Pause the in stream until the out stream is ready
            messageStream.pause();

            final MessageParser messageParser = messageStream.getMessageParser();
            final MessageFilter messageFilter = messageStream.getMessageFilter();
            inToOutPump = Pump.pump(messageParser, outStreamMux);
            outToInPump = Pump.pump(outStreamMux, messageFilter);

            messageParser.endHandler(v -> {
                if (endHandler != null) endHandler.handle(null);
                doStop();
            });
        }




        @Override
        public Channel start() {
            inToOutPump.start();
            outToInPump.start();
            messageStream.resume();
            return this;
        }



        private void doStop() {
            if (inToOutPump != null) inToOutPump.stop();
            if (outToInPump != null) outToInPump.stop();
            if (outStreamMuxPool != null) outStreamMuxPool.returnObject(outStreamMux);
        }

    }
}
