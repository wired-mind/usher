package io.cozmic.usher.plugins.kafka;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;

/**
 * Created by chuck on 11/9/15.
 */
public class HashKeyFactory implements RecordFactory {
    private static ExpressionFactory factory = ExpressionFactory.newInstance();
    private final SimpleContext context = new SimpleContext();
    private final ValueExpression topicExpression;
    private final ValueExpression keyExpression;


    public HashKeyFactory(String topic, String key) {
        topicExpression = factory.createValueExpression(context, topic, String.class);
        keyExpression = factory.createValueExpression(context, key, String.class);
    }

    @Override
    public ProducerRecord<byte[], byte[]> create(Buffer data, PipelinePack context) {
        final String dynamicTopic = (String) topicExpression.getValue(context.getRuntimeContext());
        final String dynamicKey = (String) keyExpression.getValue(context.getRuntimeContext());
        return new ProducerRecord<>(dynamicTopic, dynamicKey.getBytes(), data.getBytes());
    }
}
