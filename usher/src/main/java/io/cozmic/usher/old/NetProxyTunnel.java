package io.cozmic.usher.old;

import io.cozmic.usherprotocols.core.*;
import io.cozmic.usherprotocols.protocols.RuleBasedPacketSocket;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.ReadStream;


import java.util.UUID;

/**
 * Created by chuck on 10/24/14.
 */
public class NetProxyTunnel extends ProxyTunnel {

    private final JsonObject requestParsingRules;

    public NetProxyTunnel(Context container, Vertx vertx) {
        super(container, vertx);


        final JsonObject defaultConfigFixedTwo = new JsonObject().put("type", "fixed").put("length", 2);
        requestParsingRules = container.config().getJsonObject("requestParsingRules", defaultConfigFixedTwo);
    }

    protected ReadStream<Buffer> wrapReadStream(final NetSocket sock, final Connection connection, final CozmicPump receivePump) {
        TranslatingReadStream<Buffer> translatingReadStream = new RuleBasedPacketSocket(sock, requestParsingRules);

        return translatingReadStream.translate(new StreamProcessor() {
            @Override
            public void process(Buffer body, Handler<AsyncResult<Buffer>> resultHandler) {
                final String messageId = UUID.randomUUID().toString();

                final Request request = new Request(messageId, connection.getConnectionId(), System.currentTimeMillis(), body);
                receivePump.add(request, sock);
                resultHandler.handle(Future.succeededFuture(request.buildEnvelope()));
            }
        });
    }

    protected MessageReadStream wrapWithMessageReader(NetSocket serviceSocket) {
        return new CozmicSocket(serviceSocket);
    }
}
