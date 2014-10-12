package io.cozmic.usher.core;

import org.vertx.java.core.buffer.Buffer;

public interface MessageTranslator {
    Buffer translate(Message message);
}
