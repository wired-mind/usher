package io.cozmic.usher.plugins.file;

import com.google.common.collect.Maps;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.Map;
import java.util.UUID;

/**
 * Created by chuck on 7/13/15.
 */
public class FileOutput implements OutputPlugin {
    Logger logger = LoggerFactory.getLogger(FileOutput.class.getName());
    private JsonObject configObj;
    private Vertx vertx;
    private Map<String, FileStream> fileStreamMap = Maps.newConcurrentMap();

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        final String path = configObj.getString("path", "/tmp/" + UUID.randomUUID().toString());

        findOrCreateFileStream(path, asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain file handle for %s. Cause: %s", configObj.toString(), cause.getMessage()), cause);
                run(duplexStreamAsyncResultHandler);
                return;
            }

            final FileStream fileStream = asyncResult.result();
            duplexStreamAsyncResultHandler.handle(Future.succeededFuture(new DuplexStream<>(fileStream, fileStream, pack -> {
                final Message message = pack.getMessage();

            })));
        });



    }

    private void findOrCreateFileStream(String path, AsyncResultHandler<FileStream> asyncResultHandler) {
        FileStream fileStream = fileStreamMap.get(path);
        if (fileStream != null) {
            asyncResultHandler.handle(Future.succeededFuture(fileStream));
            return;
        }
        OpenOptions openOptions = new OpenOptions(configObj.getJsonObject("openOptions", new JsonObject()));

        vertx.fileSystem().open(path, openOptions, asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain file handle for %s. Cause: %s", configObj.toString(), cause.getMessage()), cause);
                asyncResultHandler.handle(Future.failedFuture(cause));
                return;
            }
            final AsyncFile file = asyncResult.result();
            final Buffer currentFileBuffer = vertx.fileSystem().readFileBlocking(path);
            final FileStream newFileStream = new FileStream(path, file, currentFileBuffer.length());
            fileStreamMap.put(path, newFileStream);
        });


    }

    @Override
    public void stop(OutPipeline outPipeline) {

    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }


    private class FileStream implements ReadStream<Buffer>, WriteStream<Buffer> {
        private final String path;
        private final AsyncFile file;
        private Handler<Buffer> dataHandler;
        private Handler<Void> endHandler;

        public FileStream(String path, AsyncFile file, int startWritePos) {
            this.path = path;
            this.file = file;

            file.setWritePos(startWritePos);
        }

        @Override
        public FileStream exceptionHandler(Handler<Throwable> handler) {
            file.exceptionHandler(handler);
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            file.write(data);
            if (configObj.getBoolean("successResponse", false)) {
                dataHandler.handle(Buffer.buffer(new byte[] {0x1}));
            }
            return this;
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
            file.setWriteQueueMaxSize(maxSize);
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return file.writeQueueFull();
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
            file.drainHandler(handler);
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> dataHandler) {
            this.dataHandler = dataHandler;

            return this;
        }

        @Override
        public ReadStream<Buffer> pause() {
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            return this;
        }

        @Override
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }
    }
}
