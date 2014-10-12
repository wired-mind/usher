package io.cozmic.usher.journal;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalEventProducer {
    private final RingBuffer<JournalEvent> ringBuffer;

    public JournalEventProducer(RingBuffer<JournalEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<JournalEvent, Buffer> TRANSLATOR =
            new EventTranslatorOneArg<JournalEvent, Buffer>()
            {
                public void translateTo(JournalEvent event, long sequence, Buffer buffer)
                {
                    event.setBuff(buffer);
                }
            };

    public void onData(Buffer buffer, NetSocket socket)
    {
        final long next = ringBuffer.next();
        final JournalEvent journalEvent = ringBuffer.get(next);
        journalEvent.setBuff(buffer);
        journalEvent.setSocket(socket);
        ringBuffer.publish(next);
        //ringBuffer.publishEvent(TRANSLATOR, buffer);
    }
}
