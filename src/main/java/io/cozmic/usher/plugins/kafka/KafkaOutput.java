package io.cozmic.usher.plugins.kafka;

import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.NullReadStream;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        final KafkaProducerStream kafkaProducerStream = new KafkaProducerStream(vertx, topic);
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
        kafkaProducerProps.put("bootstrap.servers", configObj.getString("bootstrap.servers", "kafka.dev:9092"));
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(kafkaProducerProps);


    }

    private class KafkaProducerStream implements ClosableWriteStream<Buffer> {
        private final Vertx vertx;
        private String topic;

        public KafkaProducerStream(Vertx vertx, String topic) {

            this.vertx = vertx;
            this.topic = topic;
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
         * @param writeCompleteHandler
         * @return
         */
        @Override
        public AsyncWriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> writeCompleteHandler) {
            Objects.requireNonNull(data, "Must provide data to write to Kafka");
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, data.getBytes());
            try {
                producer.send(record, (metadata, exception) -> vertx.runOnContext(v -> {
                    if (exception != null) {
                        writeCompleteHandler.handle(Future.failedFuture(exception));
                        return;
                    }
                    writeCompleteHandler.handle(Future.succeededFuture());
                }));
            } catch (Exception ex) {
                writeCompleteHandler.handle(Future.failedFuture(ex));
            }


            return this;
        }

        @Override
        public void close() {
            //no op
        }
    }
}
