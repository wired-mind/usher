package io.cozmic.usherprotocols.core;



import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 9/30/14.
 */
public class StreamProcessingReadStream implements ReadStream<Buffer> {
    private final ReadStream<Buffer> innerReadStream;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<Buffer> dataHandler;

    public StreamProcessingReadStream(ReadStream<Buffer> readStream, final StreamProcessor streamProcessor) {

        this.innerReadStream = readStream;

        innerReadStream.handler(new Handler<Buffer>() {
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
    public StreamProcessingReadStream handler(final Handler<Buffer> handler) {
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
