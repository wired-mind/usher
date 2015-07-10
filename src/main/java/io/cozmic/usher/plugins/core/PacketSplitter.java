package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.SplitterPlugin;
import io.cozmic.usher.vertx.parsers.RuleBasedPacketParser;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/1/15.
 */
public class PacketSplitter implements SplitterPlugin {
    private JsonObject configObj;
    private Vertx vertx;
    private RuleBasedPacketParser packetParser;

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;

        packetParser = RuleBasedPacketParser.fromConfig(configObj.getJsonObject("rules"), null);
    }

    @Override
    public void findRecord(Buffer buffer, Handler<Buffer> bufferHandler) {
        packetParser.setOutput(bufferHandler);
        packetParser.handle(buffer);
    }

    @Override
    public SplitterPlugin createNew() {
        final PacketSplitter packetSplitter = new PacketSplitter();
        packetSplitter.init(configObj, vertx);
        return packetSplitter;
    }
}
