package io.cozmic.usher.old;

import io.cozmic.usherprotocols.core.*;
import io.cozmic.usherprotocols.protocols.HttpSocket;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.ReadStream;


import java.util.UUID;

/**
 * Created by chuck on 10/24/14.
 */
public class HttpProxyTunnel extends ProxyTunnel {
    public HttpProxyTunnel(Context container, Vertx vertx) {
        super(container, vertx);
    }

    protected ReadStream<Buffer> wrapReadStream(final NetSocket sock, final Connection connection, final CozmicPump receivePump) {
        final TranslatingReadStream httpSocket = new HttpSocket(sock);


        final ReadStream<Buffer> translatingReadStream = httpSocket.translate(new StreamProcessor() {
            @Override
            public void process(Buffer data, Handler<AsyncResult<Buffer>> resultHandler) {
                final String messageId = UUID.randomUUID().toString();
                //Inject new header here
                //Update this later

                receivePump.add(new Request(messageId, connection.getConnectionId(), System.currentTimeMillis(), data), sock);
                resultHandler.handle(Future.succeededFuture(data));
            }
        });


        return translatingReadStream;
    }

    protected MessageReadStream wrapWithMessageReader(NetSocket serviceSocket) {
        //HTTP will need a derivative of this....
        return new CozmicSocket(serviceSocket);
    }}
