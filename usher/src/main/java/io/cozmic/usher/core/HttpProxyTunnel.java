package io.cozmic.usher.core;

import io.cozmic.usher.peristence.MessageEventProducer;
import io.cozmic.usherprotocols.core.*;
import io.cozmic.usherprotocols.protocols.HttpSocket;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.platform.Container;

import java.util.UUID;

/**
 * Created by chuck on 10/24/14.
 */
public class HttpProxyTunnel extends ProxyTunnel {
    public HttpProxyTunnel(Container container, Vertx vertx, final MessageEventProducer journalProducer, final MessageEventProducer timeoutLogProducer) {
        super(container, vertx, journalProducer, timeoutLogProducer);
    }

    protected ReadStream<?> wrapReadStream(final NetSocket sock, final CozmicPump receivePump) {
        final TranslatingReadStream httpSocket = new HttpSocket(sock);


        final ReadStream<?> translatingReadStream = httpSocket.translate(new StreamProcessor() {
            @Override
            public void process(Buffer data, Handler<AsyncResult<Buffer>> resultHandler) {
                final String messageId = UUID.randomUUID().toString();
                //Inject new header here
                //Update this later

                receivePump.add(new Message(messageId, data), sock);
                resultHandler.handle(new DefaultFutureResult<Buffer>(data));
            }
        });


        return translatingReadStream;
    }

    protected MessageReadStream<?> wrapWithMessageReader(NetSocket serviceSocket) {
        //HTTP will need a derivative of this....
        return new CozmicSocket(serviceSocket);
    }}
