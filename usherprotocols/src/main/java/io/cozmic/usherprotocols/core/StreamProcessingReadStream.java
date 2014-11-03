package io.cozmic.usherprotocols.core;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 9/30/14.
 */
public class StreamProcessingReadStream implements ReadStream<StreamProcessingReadStream> {
    private final ReadStream<?> innerReadStream;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<Buffer> dataHandler;

    public StreamProcessingReadStream(ReadStream<?> readStream, final StreamProcessor streamProcessor) {

        this.innerReadStream = readStream;

        innerReadStream.dataHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer event) {
                streamProcessor.process(event, new Handler<AsyncResult<Buffer>>() {
                    @Override
                    public void handle(AsyncResult<Buffer> asyncResult) {
                        if (asyncResult.failed()) {
                            //TODO: real logging
                            System.out.println(asyncResult.cause().getMessage());
                            return;
                        }

                        final Buffer result = asyncResult.result();
                        if (result == null) return;

                        if (dataHandler != null) dataHandler.handle(result);
                    }
                });
            }
        });
    }

    @Override
    public StreamProcessingReadStream endHandler(Handler endHandler) {
        innerReadStream.endHandler(endHandler);
        return this;
    }

    @Override
    public StreamProcessingReadStream dataHandler(final Handler<Buffer> handler) {
        this.dataHandler = handler;
        return this;
    }


    @Override
    public StreamProcessingReadStream pause() {
        //System.out.println("Pausing StreamProcessing");
        paused = true;
        innerReadStream.pause();
        return this;
    }

    @Override
    public StreamProcessingReadStream resume() {
     //   System.out.println("Resuming StreamProcessing");
        paused = false;
        innerReadStream.resume();
        return this;
    }

    @Override
    public StreamProcessingReadStream exceptionHandler(Handler handler) {
        innerReadStream.exceptionHandler(handler);
        return this;
    }
}
