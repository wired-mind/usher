package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventHandler;
import io.cozmic.usher.PersistenceVerticle;
import io.cozmic.usherprotocols.core.Message;
import org.vertx.java.core.Vertx;

/**
 * Created by chuck on 10/31/14.
 */
public class TimeoutMessageEventHandler implements EventHandler<MessageEvent>
{

    private final Vertx vertx;

    public TimeoutMessageEventHandler(Vertx vertx) {

        this.vertx = vertx;
    }
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch)
    {
        final Message message = event.getMessage();
        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_ADDRESS, message.buildEnvelope());
    }
}