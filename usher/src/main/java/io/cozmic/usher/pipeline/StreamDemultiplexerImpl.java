package io.cozmic.usher.pipeline;

import com.google.common.collect.Lists;
import io.cozmic.usher.core.MessageConsumer;
import io.cozmic.usher.core.MessageParser;
import io.cozmic.usher.core.StreamDemultiplexer;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by chuck on 7/3/15.
 */
public class StreamDemultiplexerImpl implements StreamDemultiplexer {

    private final Vertx vertx;
    private List<DemuxRegistration> demuxes = Lists.newArrayList();
    private Handler<Void> drainHandler;

    public StreamDemultiplexerImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public StreamDemultiplexer exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    @Override
    public MessageConsumer consumer(MessageStream outputMessageStream, boolean bidirectional) {
        final DemuxRegistration messageConsumer = new DemuxRegistration(outputMessageStream, bidirectional);
        demuxes.add(messageConsumer);

        return messageConsumer;
    }


    private void unregisterDemux(DemuxRegistration demuxRegistration) {
        Objects.requireNonNull(demuxRegistration, "Must specify the registration to unregister.");
        demuxes.remove(demuxRegistration);
    }


    @Override
    public void unregisterAllConsumers() {
        for (MessageConsumer messageConsumer : demuxes) {
            messageConsumer.unregister();
        }
    }

    @Override
    public List<MessageParser> getResponseStreams() {
        final ArrayList<MessageParser> responseStreams = Lists.newArrayList();
        for (DemuxRegistration demux : demuxes) {
            final MessageParser responseStream = demux.getResponseStream();
            if (responseStream != null) responseStreams.add(responseStream);
        }

        return responseStreams;
    }

    @Override
    public WriteStream<Message> write(Message data) {
        for (DemuxRegistration demux : demuxes) {
            demux.handle(data);        //TODO: probably want to clone data. not doing it just yet. we'll see
        }

        return this;
    }

    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        for (DemuxRegistration demux : demuxes) {
            if (demux.isPaused()) return true;
        }

        return false;
    }

    @Override
    public WriteStream<Message> drainHandler(Handler<Void> drainHandler) {
        this.drainHandler = drainHandler;
        vertx.runOnContext(v -> callDrainHandler());
        return this;
    }
    private synchronized void callDrainHandler() {
        if (drainHandler != null) {
            if (!writeQueueFull()) {
                drainHandler.handle(null);
            }
        }
    }

    /**
     * Created by chuck on 7/6/15.
     */
    public class DemuxRegistration implements MessageConsumer, Handler<Message> {
        private MessageParser responseStream;
        private Pump pump;
        private Handler<Message> handler;
        private Handler<Void> endHandler;
        private boolean paused;
        private Handler<Throwable> exceptionHandler;

        public DemuxRegistration(MessageStream outputMessageStream, boolean bidirectional) {
            //TODO: clone/wrap this
            pump = Pump.pump(this, outputMessageStream.getMessageFilter()).start();
            if (bidirectional) {
                responseStream = outputMessageStream.getMessageParser();
            }
        }

        @Override
        public MessageParser getResponseStream() {
            return responseStream;
        }

        @Override
        public void unregister() {
            pump.stop();
            unregisterDemux(this);
            if (endHandler != null) endHandler.handle(null);
        }


        @Override
        public ReadStream<Message> exceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        @Override
        public ReadStream<Message> handler(Handler<Message> handler) {
            this.handler = handler;
            return this;
        }

        @Override
        public ReadStream<Message> pause() {
            paused = true;
            return this;
        }

        @Override
        public ReadStream<Message> resume() {
            paused = false;
            vertx.runOnContext(v -> callDrainHandler());
            return this;
        }

        @Override
        public ReadStream<Message> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }


        @Override
        public void handle(Message data) {
            if (handler != null) handler.handle(data);
        }

        public boolean isPaused() {
            return paused;
        }
    }
}
