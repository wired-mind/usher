package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.core.SplitterPlugin;
import io.cozmic.usherprotocols.core.Message;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 6/29/15.
 */
public class MessageParserImpl implements MessageParser, Handler<Buffer> {
    private final SplitterPlugin splitterPlugin;
    private final DecoderPlugin decoderPlugin;

    private final ReadStream<Buffer> innerReadStream;
    private ConcurrentLinkedQueue<Message> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<Message> handler;

    public MessageParserImpl(ReadStream<Buffer> readStream, SplitterPlugin splitterPlugin, DecoderPlugin decoderPlugin) {
        innerReadStream = readStream;
        this.splitterPlugin = splitterPlugin;
        this.decoderPlugin = decoderPlugin;

        readStream.handler(this);
    }

    @Override
    public void handle(Buffer buffer) {
        splitterPlugin.findRecord(buffer, record -> {
            decoderPlugin.decode(record, message -> {
                readBuffers.add(message);

                if (paused) {
                    return;
                }

                purgeReadBuffers();
            });
        });
    }


    protected void purgeReadBuffers() {
        while (!readBuffers.isEmpty() && !paused) {
            final Message nextMessage = readBuffers.poll();
            if (nextMessage != null) {
                if (handler != null) handler.handle(nextMessage);
            }
        }
    }


    @Override
    public ReadStream<Message> endHandler(Handler<Void> endHandler) {
        innerReadStream.endHandler(endHandler);
        return this;
    }

    @Override
    public ReadStream<Message> handler(final Handler<Message> handler) {
        this.handler = handler;
        return this;
    }


    @Override
    public ReadStream<Message> pause() {
        paused = true;
        innerReadStream.pause();
        return this;
    }

    @Override
    public ReadStream<Message> resume() {
        paused = false;
        purgeReadBuffers();
        innerReadStream.resume();
        return this;
    }

    @Override
    public ReadStream<Message> exceptionHandler(Handler<Throwable> handler) {
        innerReadStream.exceptionHandler(handler);
        return this;
    }
}
