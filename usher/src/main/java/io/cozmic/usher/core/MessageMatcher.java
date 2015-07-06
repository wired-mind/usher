package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;

/**
 * Created by chuck on 7/3/15.
 */
public interface MessageMatcher {
    boolean matches(Message message);

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
        public boolean matches(Message message) {
            return defaultVal;
        }
    }
}
