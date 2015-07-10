package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.core.FramingSplitter;
import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.core.SplitterPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 6/29/15.
 */
public class DefaultInPipeline implements InPipeline, Handler<Buffer> {
    private final SplitterPlugin splitterPlugin;
    private final DecoderPlugin decoderPlugin;

    private final ReadStream<Buffer> innerReadStream;
    private final DuplexStream<Buffer, Buffer> duplexStream;
    private final JsonObject splitterConfig;
    private final JsonObject decoderConfig;
    private ConcurrentLinkedQueue<PipelinePack> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<PipelinePack> handler;
    private Boolean useMsgBytes;
    private Handler<PipelinePack> deliveryHandler;

    public DefaultInPipeline(DuplexStream<Buffer, Buffer> duplexStream, JsonObject splitterConfig, SplitterPlugin splitterPlugin, JsonObject decoderConfig, DecoderPlugin decoderPlugin) {
        this.duplexStream = duplexStream;
        this.splitterConfig = splitterConfig;
        this.decoderConfig = decoderConfig;
        innerReadStream = duplexStream.getReadStream();
        this.splitterPlugin = splitterPlugin;
        this.decoderPlugin = decoderPlugin;

        final boolean useMessageBytesDefault = splitterPlugin instanceof FramingSplitter;
        useMsgBytes = splitterConfig.getBoolean("useMessageBytes", useMessageBytesDefault);

        innerReadStream.handler(this);

        deliveryHandler = pack -> {
            readBuffers.add(pack);

            if (paused) {
                return;
            }

            purgeReadBuffers();
        };
    }

    @Override
    public void handle(Buffer buffer) {

        splitterPlugin.findRecord(buffer, record -> {
            final PipelinePack pipelinePack;
            if (useMsgBytes) {
                pipelinePack = new PipelinePack(record);
            } else {
                final Message message = new Message();
                message.setPayload(record.toString());
                message.setMessageId(UUID.randomUUID());
                message.setTimestamp(System.currentTimeMillis());
                pipelinePack = new PipelinePack(message);
            }


            decoderPlugin.decode(pipelinePack, pack -> {
                if (pack.getMessage() instanceof Message) {
                    duplexStream.decorate(pack, deliveryHandler);
                } else {
                    deliveryHandler.handle(pack);
                }
            });
        });
    }


    protected void purgeReadBuffers() {
        while (!readBuffers.isEmpty() && !paused) {
            final PipelinePack nextPack = readBuffers.poll();
            if (nextPack != null) {
                if (handler != null) handler.handle(nextPack);
            }
        }
    }


    @Override
    public ReadStream<PipelinePack> endHandler(Handler<Void> endHandler) {
        innerReadStream.endHandler(endHandler);
        return this;
    }

    @Override
    public ReadStream<PipelinePack> handler(final Handler<PipelinePack> handler) {
        this.handler = handler;
        if (handler != null) purgeReadBuffers();
        return this;
    }


    @Override
    public ReadStream<PipelinePack> pause() {
        paused = true;
        innerReadStream.pause();
        return this;
    }

    @Override
    public ReadStream<PipelinePack> resume() {
        paused = false;
        purgeReadBuffers();
        innerReadStream.resume();
        return this;
    }

    @Override
    public ReadStream<PipelinePack> exceptionHandler(Handler<Throwable> handler) {
        innerReadStream.exceptionHandler(handler);
        return this;
    }
}
