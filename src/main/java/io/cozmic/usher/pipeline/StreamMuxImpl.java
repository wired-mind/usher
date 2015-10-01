package io.cozmic.usher.pipeline;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cozmic.usher.core.CountDownFutureResult;
import io.cozmic.usher.core.MuxRegistration;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.StreamMux;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.List;
import java.util.Objects;

/**
 * Created by chuck on 7/3/15.
 */
public class StreamMuxImpl implements StreamMux {

    private final Vertx vertx;
    private List<MuxRegistrationImpl> demuxes = Lists.newCopyOnWriteArrayList();
    private Handler<Void> drainHandler;
    private Handler<PipelinePack> messageHandler;
    private boolean muxPaused;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private Handler<PipelinePack> writeCompleteHandler;

    public StreamMuxImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public StreamMux exceptionHandler(Handler<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        for (MuxRegistration muxRegistration : demuxes) {
            muxRegistration.exceptionHandler(exceptionHandler);
        }
        return this;
    }

    @Override
    public ReadStream<PipelinePack> handler(Handler<PipelinePack> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public ReadStream<PipelinePack> pause() {
        muxPaused = true;
        return this;
    }

    @Override
    public ReadStream<PipelinePack> resume() {
        muxPaused = false;
        for (MuxRegistrationImpl demux : demuxes) {
            vertx.runOnContext(v -> demux.callDrainHandler());
        }

        return this;
    }

    @Override
    public ReadStream<PipelinePack> endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public MuxRegistration addStream(MessageStream messageStream, boolean bidirectional) {
        final MuxRegistrationImpl muxRegistration = new MuxRegistrationImpl(messageStream, bidirectional);
        demuxes.add(muxRegistration);

        return muxRegistration;
    }


    private void unregisterDemux(MuxRegistrationImpl muxRegistration) {
        Objects.requireNonNull(muxRegistration, "Must specify the registration to unregister.");
        demuxes.remove(muxRegistration);
    }


    @Override
    public void unregisterAllConsumers() {
        for (MuxRegistration muxRegistration : demuxes) {
            muxRegistration.unregister();
        }
        if (endHandler != null) endHandler.handle(null);
    }

    @Override
    public WriteStream<PipelinePack> write(PipelinePack data) {
        return write(data, asyncResult -> {
            if (asyncResult.failed()) {
                if (exceptionHandler != null) exceptionHandler.handle(asyncResult.cause());
                return;
            }
            if (writeCompleteHandler != null) writeCompleteHandler.handle(asyncResult.result());
        });
    }

    /**
     * Special version of write that asynchronously signals when the underlying writes are complete.
     * @param data
     * @param doneHandler
     * @return
     */
    @Override
    public StreamMux write(PipelinePack data, Handler<AsyncResult<PipelinePack>> doneHandler) {
        final Iterable<MuxRegistrationImpl> matchingStreams = findMatchingStreams(data);
        final int countOfMatchingStreams = Iterables.size(matchingStreams);
        final CountDownFutureResult<PipelinePack> doneFuture = new CountDownFutureResult<>(countOfMatchingStreams);
        doneFuture.setHandler(doneHandler);
        for (MuxRegistrationImpl demux : matchingStreams) {
            demux.handle(data, doneFuture);        //TODO: probably want to clone data. not doing it just yet. we'll see
        }

        return this;
    }

    @Override
    public StreamMux writeCompleteHandler(Handler<PipelinePack> writeCompleteHandler) {
        this.writeCompleteHandler = writeCompleteHandler;
        return this;
    }


    private Iterable<MuxRegistrationImpl> findMatchingStreams(PipelinePack data) {
        return Iterables.filter(demuxes, new RoutingPredicate(data));
    }

    @Override
    public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        for (MuxRegistrationImpl demux : demuxes) {
            if (demux.paused) return true;
        }

        return false;
    }

    @Override
    public WriteStream<PipelinePack> drainHandler(Handler<Void> drainHandler) {
        this.drainHandler = drainHandler;
        vertx.runOnContext(v -> callMuxDrainHandler());
        return this;
    }
    private synchronized void callMuxDrainHandler() {
        if (drainHandler != null) {
            if (!writeQueueFull()) {
                drainHandler.handle(null);
            }
        }
    }

    private static class RoutingPredicate implements Predicate<MuxRegistration> {
        private final PipelinePack data;

        public RoutingPredicate(PipelinePack data) {
            this.data = data;
        }

        @Override
        public boolean apply(MuxRegistration input) {
            return input.matches(data);


        }
    }

    /**
     * Created by chuck on 7/6/15.
     */
    public class MuxRegistrationImpl implements MuxRegistration, Handler<PipelinePack> {
        private final MessageStream messageStream;
        private Pump demuxPump;
        private Pump muxPump;
        private Handler<PipelinePack> handler;
        private Handler<Void> endHandler;
        private boolean paused;
        private Handler<Throwable> exceptionHandler;
        private Handler<Void> drainHandler;
        private OutPipeline outPipeline;

        public MuxRegistrationImpl(MessageStream messageStream, boolean bidirectional) {
            this.messageStream = messageStream;

            outPipeline = messageStream.getOutPipeline();
            demuxPump = Pump.pump(this, outPipeline).start();

            if (bidirectional) {
                muxPump = Pump.pump(messageStream.getInPipeline(), this).start();
            }
        }

        @Override
        public void unregister() {
            demuxPump.stop();
            muxPump.stop();
            unregisterDemux(this);
            if (endHandler != null) endHandler.handle(null);
        }


        @Override
        public MuxRegistration exceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            messageStream.getOutPipeline().exceptionHandler(exceptionHandler);
            messageStream.getInPipeline().exceptionHandler(exceptionHandler);
            return this;
        }

        @Override
        public boolean matches(PipelinePack pack) {
            return messageStream.matches(pack);
        }

        @Override
        public WriteStream<PipelinePack> write(PipelinePack data) {
            if (messageHandler != null) messageHandler.handle(data);
            return this;
        }

        @Override
        public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return muxPaused;
        }

        @Override
        public WriteStream<PipelinePack> drainHandler(Handler<Void> drainHandler) {
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

        @Override
        public ReadStream<PipelinePack> handler(Handler<PipelinePack> handler) {
            this.handler = handler;
            return this;
        }

        @Override
        public ReadStream<PipelinePack> pause() {
            paused = true;
            return this;
        }

        @Override
        public ReadStream<PipelinePack> resume() {
            paused = false;
            vertx.runOnContext(v -> callMuxDrainHandler());
            return this;
        }

        @Override
        public ReadStream<PipelinePack> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }


        @Override
        public void handle(PipelinePack data) {
            if (handler != null) handler.handle(data);
        }

        public void handle(PipelinePack data, Future<PipelinePack> doneFuture) {
            outPipeline.writeCompleteHandler(asyncResult -> {
                if (asyncResult.failed()) {
                    doneFuture.fail(asyncResult.cause());
                    return;
                }
                doneFuture.complete(data);
            });
            handle(data);
        }
    }
}
