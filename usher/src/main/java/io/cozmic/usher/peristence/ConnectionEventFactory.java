package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventFactory;

/**
 * Created by chuck on 9/15/14.
 */
public class ConnectionEventFactory implements EventFactory<ConnectionEvent>
{
    public ConnectionEvent newInstance()
    {
        return new ConnectionEvent();
    }
}
