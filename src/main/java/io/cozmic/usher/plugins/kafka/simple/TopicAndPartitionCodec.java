package io.cozmic.usher.plugins.kafka.simple;

import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import kafka.common.TopicAndPartition;

/**
 * TopicAndPartitionCodec
 * Created by Craig Earley on 10/4/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class TopicAndPartitionCodec implements MessageCodec<TopicAndPartition, TopicAndPartition> {

    // See https://github.com/eclipse/vert.x/tree/master/src/main/java/io/vertx/core/eventbus/impl/codecs

    @Override
    public void encodeToWire(Buffer buffer, TopicAndPartition topicAndPartition) {
        String s = String.format("%s,%d", topicAndPartition.topic(), topicAndPartition.partition());
        byte[] strBytes = s.getBytes(CharsetUtil.UTF_8);
        buffer.appendInt(strBytes.length);
        buffer.appendBytes(strBytes);
    }

    @Override
    public TopicAndPartition decodeFromWire(int pos, Buffer buffer) {
        int length = buffer.getInt(pos);
        pos += 4;
        byte[] bytes = buffer.getBytes(pos, pos + length);
        String[] s = (new String(bytes, CharsetUtil.UTF_8)).split(",");
        return new TopicAndPartition(s[0], Integer.getInteger(s[1]));
    }

    @Override
    public TopicAndPartition transform(TopicAndPartition topicAndPartition) {
        return new TopicAndPartition(topicAndPartition.topic(), topicAndPartition.partition());
    }

    @Override
    public String name() {
        return TopicAndPartition.class.getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
