package io.cozmic.usher;

import io.cozmic.usher.core.*;
import io.cozmic.usher.protocols.HttpClientRequestProcessor;
import io.cozmic.usher.protocols.HttpMessageProcessor;
import io.cozmic.usher.protocols.HttpReceiveProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 9/17/14.
 */
public class HttpProxy extends Proxy {
    @Override
    protected ReceiveProcessor createReceiveProcessor() {
        return new HttpReceiveProcessor();
    }

    @Override
    protected MessageProcessingPump createSendPump(ReadStream<?> readStream, WriteStream<?> writeStream, MessageTranslator translator) {
        final MessageProcessorPipeline pipeline = new MessageProcessorPipeline(new HttpMessageProcessor(), new HttpClientRequestProcessor());

        return MessageProcessingPump.createPump(readStream, writeStream, pipeline, translator);
    }

}
