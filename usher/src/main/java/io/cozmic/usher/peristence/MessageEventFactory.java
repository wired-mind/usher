package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventFactory;

/**
 * Created by chuck on 9/15/14.
 */
public class MessageEventFactory implements EventFactory<MessageEvent>
{
    public MessageEvent newInstance()
    {
        return new MessageEvent();
    }
}
