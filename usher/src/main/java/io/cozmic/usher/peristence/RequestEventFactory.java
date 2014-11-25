package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventFactory;

/**
 * Created by chuck on 9/15/14.
 */
public class RequestEventFactory implements EventFactory<RequestEvent>
{
    public RequestEvent newInstance()
    {
        return new RequestEvent();
    }
}
