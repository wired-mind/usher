package io.cozmic.usher.plugins.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import kafka.common.TopicAndPartition;

/**
 * KafkaCommitHandler
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@FunctionalInterface
public interface KafkaCommitHandler {
    void handle(TopicAndPartition topicAndPartition, Long offset, Handler<AsyncResult<Void>> asyncResultHandler);
}
