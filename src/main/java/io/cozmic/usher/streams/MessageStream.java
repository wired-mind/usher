package io.cozmic.usher.streams;


import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.message.PipelinePack;

import java.util.Objects;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageStream extends DuplexStream<PipelinePack, PipelinePack> {
    private final InPipeline inPipeline;
    private final OutPipeline outPipeline;
    private MessageMatcher messageMatcher;

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


    public void setMessageMatcher(MessageMatcher messageMatcher) {
        this.messageMatcher = messageMatcher;
    }

    public MessageMatcher getMessageMatcher() {
        return messageMatcher;
    }

    public boolean matches(PipelinePack pack) {
        Objects.requireNonNull(messageMatcher, "MessageMatcher must be set.");
        return messageMatcher.matches(pack);
    }
}
