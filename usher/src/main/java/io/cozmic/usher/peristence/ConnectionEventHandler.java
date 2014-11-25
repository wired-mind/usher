package io.cozmic.usher.peristence;
import com.lmax.disruptor.EventHandler;
import io.cozmic.usher.PersistenceVerticle;
import io.cozmic.usherprotocols.core.Connection;
import org.vertx.java.core.Vertx;

/**
 * Created by chuck on 9/15/14.
 */
public class ConnectionEventHandler implements EventHandler<ConnectionEvent>
{

    private final Vertx vertx;

    public ConnectionEventHandler(Vertx vertx) {

        this.vertx = vertx;
    }
    public void onEvent(ConnectionEvent event, long sequence, boolean endOfBatch)
    {
        final Connection connection = event.getConnection();
        vertx.eventBus().send(PersistenceVerticle.CONNECTION_LOG_ADDRESS, connection.asBuffer());
    }
}