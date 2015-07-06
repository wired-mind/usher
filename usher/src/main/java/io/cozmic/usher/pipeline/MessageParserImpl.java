package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.core.MessageParser;
import io.cozmic.usher.core.SplitterPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
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
    private final DuplexStream<Buffer, Buffer> duplexStream;
    private ConcurrentLinkedQueue<Message> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<Message> handler;

    public MessageParserImpl(DuplexStream<Buffer, Buffer> duplexStream, SplitterPlugin splitterPlugin, DecoderPlugin decoderPlugin) {
        this.duplexStream = duplexStream;
        innerReadStream = duplexStream.getReadStream();
        this.splitterPlugin = splitterPlugin;
        this.decoderPlugin = decoderPlugin;

        innerReadStream.handler(this);
    }

    @Override
    public void handle(Buffer buffer) {
        splitterPlugin.findRecord(buffer, record -> {
            decoderPlugin.decode(record, message -> {
                duplexStream.decorate(message, decorated -> {
                    readBuffers.add(decorated);

                    if (paused) {
                        return;
                    }

                    purgeReadBuffers();
                });

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
        if (handler != null) purgeReadBuffers();
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
