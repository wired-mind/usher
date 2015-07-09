package io.cozmic.usher.streams;


import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.pipeline.FilteredOutPipeline;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageStream extends DuplexStream<Message, Message> {
    private final InPipeline inPipeline;
    private final OutPipeline outPipeline;

    public MessageStream(InPipeline inPipeline, OutPipeline outPipeline) {
        super(inPipeline, outPipeline);
        this.inPipeline = inPipeline;
        this.outPipeline = outPipeline;
    }

    public InPipeline getInPipeline() {
        return inPipeline;
    }

    public OutPipeline getOutPipeline() {
        return outPipeline;
    }


    public MessageStream filterBy(MessageMatcher messageMatcher) {
        return new MessageStream(inPipeline, new FilteredOutPipeline(outPipeline, messageMatcher));
    }
}
