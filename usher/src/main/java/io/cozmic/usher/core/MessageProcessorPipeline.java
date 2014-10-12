package io.cozmic.usher.core;

import org.vertx.java.core.Handler;

/**
 * Created by chuck on 9/17/14.
 */
public class MessageProcessorPipeline implements MessageProcessor {

    private final MessageProcessor firstProcessor;
    private MessageProcessor lastProcessor = null;

    public MessageProcessorPipeline(MessageProcessor... processors) {
        if (processors == null || processors.length == 0) {
            throw new IllegalArgumentException("Must provide at least one processor");
        }
        firstProcessor = processors[0];
        for (MessageProcessor processor : processors) {
            if (lastProcessor != null) {
                lastProcessor.setOutput(processor);
            }

            lastProcessor = processor;
        }
    }

    @Override
    public void setOutput(Handler<Message> output) {
        this.lastProcessor.setOutput(output);
    }

    @Override
    public void handle(Message message) {
        firstProcessor.handle(message);
    }
}
