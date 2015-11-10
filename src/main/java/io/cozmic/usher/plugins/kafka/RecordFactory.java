package io.cozmic.usher.plugins.kafka;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by chuck on 11/9/15.
 */
public interface RecordFactory {
    ProducerRecord<byte[],byte[]> create(Buffer data, PipelinePack context);
}
