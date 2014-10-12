package io.cozmic.usher;

import io.cozmic.usher.core.*;
import io.cozmic.usher.protocols.ConfigurablePacketLengthMessageProcessor;
import io.cozmic.usher.protocols.NetReceiveProcessor;
import io.cozmic.usher.protocols.NetSendProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 9/17/14.
 *
 * Right now this class uses a hardcoded strategy for parsing tcp packets that matches the needs of the Splitscnd project,
 * ConfigurablePacketLengthMessageProcessor. This can be made dynamic later.
 */
public class NetProxy extends Proxy {

    @Override
    protected ReceiveProcessor createReceiveProcessor() {
        return new NetReceiveProcessor();
    }

    @Override
    protected MessageProcessingPump createSendPump(ReadStream<?> readStream, WriteStream<?> writeStream, MessageTranslator translator) {
        final MessageProcessorPipeline pipeline = new MessageProcessorPipeline(new ConfigurablePacketLengthMessageProcessor(), new NetSendProcessor());

        return MessageProcessingPump.createPump(readStream, writeStream, pipeline, translator);
    }


}
