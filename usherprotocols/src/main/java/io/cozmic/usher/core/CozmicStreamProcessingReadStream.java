package io.cozmic.usher.core;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
* Created by chuck on 9/30/14.
*/
public class CozmicStreamProcessingReadStream implements ReadStream<CozmicStreamProcessingReadStream>, MessageReadStream<CozmicStreamProcessingReadStream> {
    private final CozmicSocket cozmicSocket;
    private ConcurrentLinkedQueue<Message> readBuffers = new ConcurrentLinkedQueue<>();
    private boolean paused;
    private Handler<Buffer> dataHandler;
    private Handler<Message> messageHandler;

    public CozmicStreamProcessingReadStream(CozmicSocket cozmicSocket, final CozmicStreamProcessor cozmicStreamProcessor) {

        this.cozmicSocket = cozmicSocket;

        cozmicSocket.messageHandler(new Handler<Message>() {
            @Override
            public void handle(Message message) {
                cozmicStreamProcessor.process(message, new Handler<AsyncResult<Message>>() {
                    @Override
                    public void handle(AsyncResult<Message> asyncResult) {
                        if (asyncResult.failed()) {
                            //TODO: real logging
                            System.out.println(asyncResult.cause().getMessage());
                            return;
                        }

                        final Message result = asyncResult.result();
                        if (result == null) return;

                        if (dataHandler != null) dataHandler.handle(result.getData());
                        if (messageHandler != null) messageHandler.handle(result);

                    }
                });
            }
        });
    }

    @Override
    public CozmicStreamProcessingReadStream endHandler(Handler endHandler) {
        cozmicSocket.endHandler(endHandler);
        return this;
    }

    @Override
    public CozmicStreamProcessingReadStream dataHandler(final Handler<Buffer> handler) {
        this.dataHandler = handler;
        return this;
    }
    @Override
    public CozmicStreamProcessingReadStream messageHandler(Handler<Message> messageHandler) {

        this.messageHandler = messageHandler;
        return this;
    }


    @Override
    public CozmicStreamProcessingReadStream pause() {
      //  System.out.println("Pausing StreamProcessing");
        paused = true;
        cozmicSocket.pause();
        return this;
    }

    @Override
    public CozmicStreamProcessingReadStream resume() {
   //     System.out.println("Resuming StreamProcessing");
        paused = false;
        cozmicSocket.resume();
        return this;
    }

    @Override
    public CozmicStreamProcessingReadStream exceptionHandler(Handler handler) {
        cozmicSocket.exceptionHandler(handler);
        return this;
    }
}
