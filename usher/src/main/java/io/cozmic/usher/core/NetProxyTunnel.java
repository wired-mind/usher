package io.cozmic.usher.core;

import io.cozmic.usher.peristence.ConnectionEventProducer;
import io.cozmic.usher.peristence.RequestEventProducer;
import io.cozmic.usherprotocols.core.*;
import io.cozmic.usherprotocols.protocols.ConfigurablePacketSocket;
import io.cozmic.usherprotocols.protocols.FixedLengthSocket;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.platform.Container;

import java.util.UUID;

/**
 * Created by chuck on 10/24/14.
 */
public class NetProxyTunnel extends ProxyTunnel {

    private final JsonArray packetMap;
    private final int fixedSize;

    public NetProxyTunnel(Container container, Vertx vertx, final ConnectionEventProducer connectionProducer, final RequestEventProducer journalProducer, final RequestEventProducer timeoutLogProducer) {
        super(container, vertx, connectionProducer, journalProducer, timeoutLogProducer);
        packetMap = container.config().getArray("packet_map");
        fixedSize = container.config().getNumber("fixed_size", 4).intValue();
    }

    protected ReadStream<?> wrapReadStream(final NetSocket sock, final Connection connection, final CozmicPump receivePump) {
        TranslatingReadStream<?> translatingReadStream = null;
        if (packetMap != null) {
            translatingReadStream = new ConfigurablePacketSocket(sock, packetMap);
        } else {
            translatingReadStream = new FixedLengthSocket(sock, fixedSize);
        }


        return translatingReadStream.translate(new StreamProcessor() {
            @Override
            public void process(Buffer body, Handler<AsyncResult<Buffer>> resultHandler) {
                final String messageId = UUID.randomUUID().toString();

                final Request request = new Request(messageId, connection.getConnectionId(), System.currentTimeMillis(), body);
                receivePump.add(request, sock);
                resultHandler.handle(new DefaultFutureResult<>(request.buildEnvelope()));
            }
        });
    }

    protected MessageReadStream<?> wrapWithMessageReader(NetSocket serviceSocket) {
        return new CozmicSocket(serviceSocket);
    }
}
