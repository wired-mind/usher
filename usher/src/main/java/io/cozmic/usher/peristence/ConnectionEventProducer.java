package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.cozmic.usherprotocols.core.Connection;


public class ConnectionEventProducer {
    private final RingBuffer<ConnectionEvent> ringBuffer;

    public ConnectionEventProducer(RingBuffer<ConnectionEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<ConnectionEvent, Connection> TRANSLATOR =
            new EventTranslatorOneArg<ConnectionEvent, Connection>()
            {
                public void translateTo(ConnectionEvent event, long sequence, Connection connection)
                {
                    event.setConnection(connection);
                }
            };

    public void onData(Connection connection)
    {
        ringBuffer.publishEvent(TRANSLATOR, connection);
    }
}
