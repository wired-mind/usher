package io.cozmic.usher.journal;
import com.lmax.disruptor.EventHandler;
import io.cozmic.usher.Journaler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalEventHandler implements EventHandler<JournalEvent>
{

    private final Vertx vertx;

    public JournalEventHandler(Vertx vertx) {

        this.vertx = vertx;
    }
    public void onEvent(JournalEvent event, long sequence, boolean endOfBatch)
    {
        final Buffer buff = event.getBuff();
        //final boolean eventLoop = vertx.isEventLoop();
        //final Thread thread = Thread.currentThread();
        //event.getSocket().write(buff);
        vertx.eventBus().send(Journaler.ADDRESS, buff);
    }
}