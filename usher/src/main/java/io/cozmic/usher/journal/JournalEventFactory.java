package io.cozmic.usher.journal;

import com.lmax.disruptor.EventFactory;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalEventFactory implements EventFactory<JournalEvent>
{
    public JournalEvent newInstance()
    {
        return new JournalEvent();
    }
}
