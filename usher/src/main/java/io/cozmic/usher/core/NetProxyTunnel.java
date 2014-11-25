package io.cozmic.usher.core;

import io.cozmic.usher.peristence.ConnectionEventProducer;
import io.cozmic.usher.peristence.RequestEventProducer;
import io.cozmic.usherprotocols.core.*;
import io.cozmic.usherprotocols.protocols.RuleBasedPacketSocket;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.platform.Container;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Created by chuck on 10/24/14.
 */
public class NetProxyTunnel extends ProxyTunnel {

    private final JsonObject requestParsingRules;

    public NetProxyTunnel(Container container, Vertx vertx, final ConnectionEventProducer connectionProducer, final RequestEventProducer journalProducer, final RequestEventProducer timeoutLogProducer) {
        super(container, vertx, connectionProducer, journalProducer, timeoutLogProducer);

        JsonObject defaultRequestRulesConfig = new JsonObject();
        try {
            final URI uri = getClass().getResource("/request_parsing_rules.json").toURI();
            final String requestRules = new String(Files.readAllBytes(Paths.get(uri)));
            defaultRequestRulesConfig = new JsonObject(requestRules);
        } catch (URISyntaxException | IOException e) {
            container.logger().warn("Cannot load default request parsing rules", e);
        }

        requestParsingRules = container.config().getObject("requestParsingRules", defaultRequestRulesConfig);
    }

    protected ReadStream<?> wrapReadStream(final NetSocket sock, final Connection connection, final CozmicPump receivePump) {
        TranslatingReadStream<?> translatingReadStream = new RuleBasedPacketSocket(sock, requestParsingRules);

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
