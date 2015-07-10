package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;

/**
 * Created by chuck on 7/3/15.
 */
public interface MessageMatcher {
    boolean matches(PipelinePack pipelinePack);

    static MessageMatcher always() {
        return new DefaultMatcher(true);
    }

    static MessageMatcher never() {
        return new DefaultMatcher(false);
    }

    static class DefaultMatcher implements MessageMatcher {
        private final boolean defaultVal;

        public DefaultMatcher(boolean defaultVal) {
            this.defaultVal = defaultVal;
        }

        @Override
        public boolean matches(PipelinePack pipelinePack) {
            return defaultVal;
        }


    }
}
