package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.cozmic.usherprotocols.core.Message;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/15/14.
 */
public class MessageEventProducer {
    private final RingBuffer<MessageEvent> ringBuffer;

    public MessageEventProducer(RingBuffer<MessageEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<MessageEvent, Message> TRANSLATOR =
            new EventTranslatorOneArg<MessageEvent, Message>()
            {
                public void translateTo(MessageEvent event, long sequence, Message message)
                {
                    event.setMessage(message);
                }
            };

    public void onData(Message message)
    {
        ringBuffer.publishEvent(TRANSLATOR, message);
    }
}
