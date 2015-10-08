package io.cozmic.usher.plugins.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * KafkaReplyHandler
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@FunctionalInterface
public interface KafkaReplyHandler {
    void handle(byte[] value, Handler<AsyncResult<Void>> asyncResultHandler);
}
