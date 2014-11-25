package io.cozmic.usher.peristence;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.cozmic.usherprotocols.core.Request;

/**
 * Created by chuck on 9/15/14.
 */
public class RequestEventProducer {
    private final RingBuffer<RequestEvent> ringBuffer;

    public RequestEventProducer(RingBuffer<RequestEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<RequestEvent, Request> TRANSLATOR =
            new EventTranslatorOneArg<RequestEvent, Request>()
            {
                public void translateTo(RequestEvent event, long sequence, Request request)
                {
                    event.setRequest(request);
                }
            };

    public void onData(Request request)
    {
        ringBuffer.publishEvent(TRANSLATOR, request);
    }
}
