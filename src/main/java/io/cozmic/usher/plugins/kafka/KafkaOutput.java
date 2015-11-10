package io.cozmic.usher.plugins.kafka;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.NullReadStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by chuck on 10/1/15.
 */
public class KafkaOutput implements OutputPlugin {
    private JsonObject configObj;

    private Vertx vertx;
    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private String key;
    private RecordFactory recordFactory;

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        final KafkaProducerStream kafkaProducerStream = new KafkaProducerStream(vertx, recordFactory);
        final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(NullReadStream.getInstance(), kafkaProducerStream);
        duplexStreamAsyncResultHandler.handle(Future.succeededFuture(duplexStream));
    }

    @Override
    public void stop(OutPipeline outPipeline) {

    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException {

        this.configObj = configObj;
        this.vertx = vertx;
        Properties kafkaProducerProps = new Properties();
        topic = configObj.getString("topic", "default");
        recordFactory = new RoundRobinFactory(topic);
        key = configObj.getString("key");
        if (key != null) {
            recordFactory = new HashKeyFactory(topic, key);
        }

        kafkaProducerProps.put("bootstrap.servers", configObj.getString("bootstrap.servers", "kafka.dev:9092"));
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(kafkaProducerProps);


    }

    private class KafkaProducerStream implements ClosableWriteStream<Buffer> {
        private final Vertx vertx;
        private final RecordFactory recordFactory;


        public KafkaProducerStream(Vertx vertx, RecordFactory recordFactory) {
            this.vertx = vertx;
            this.recordFactory = recordFactory;
        }

        @Override
        public KafkaProducerStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            return this;
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
            return this;
        }


        /**
         * Send message to Kafka with async callback on the vertx eventloop.
         *
         * @param data
         * @param context
         * @return
         */
        @Override
        public AsyncWriteStream<Buffer> write(Buffer data, Future<Void> future, PipelinePack context) {
            Objects.requireNonNull(data, "Must provide data to write to Kafka");

            ProducerRecord<byte[], byte[]> record = recordFactory.create(data, context);

            try {
                producer.send(record, (metadata, exception) -> vertx.runOnContext(v -> {
                    if (exception != null) {
                        future.fail(exception);
                        return;
                    }
                    future.complete();
                }));
            } catch (Exception ex) {
                future.fail(ex);
            }


            return this;
        }

        @Override
        public void close() {
            //no op
        }
    }
}
