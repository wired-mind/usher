package io.cozmic.usher.peristence;
import com.lmax.disruptor.EventHandler;
import io.cozmic.usher.Journaler;
import io.cozmic.usherprotocols.core.Message;
import org.vertx.java.core.Vertx;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalEventHandler implements EventHandler<MessageEvent>
{

    private final Vertx vertx;

    public JournalEventHandler(Vertx vertx) {

        this.vertx = vertx;
    }
    public void onEvent(MessageEvent event, long sequence, boolean endOfBatch)
    {
        final Message message = event.getMessage();
        vertx.eventBus().send(Journaler.ADDRESS, message.buildEnvelope());
    }
}