package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.MessageStream;
import io.cozmic.usher.streams.WriteCompleteFuture;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/25/15.
 */
public class InputRunnerImpl implements InputRunner {
    private final String pluginName;
    private final JsonObject inputObj;
    private final InputPlugin inputPlugin;
    private final InPipelineFactory inOutParserFactory;
    private final OutPipelineFactoryImpl outInFilterFactory;
    private final MessageMatcher messageMatcher;
    private MessageProducer<String> completePublisher;

    public InputRunnerImpl(Vertx vertx, String pluginName, JsonObject inputObj, InputPlugin inputPlugin, MessageMatcher messageMatcher, InPipelineFactory inOutParserFactory, OutPipelineFactoryImpl outInFilterFactory) {
        this.pluginName = pluginName;
        this.inputObj = inputObj;

        this.inputPlugin = inputPlugin;
        this.messageMatcher = messageMatcher;
        this.inOutParserFactory = inOutParserFactory;
        this.outInFilterFactory = outInFilterFactory;

        String completeAddress = inputObj.getString("complete.address", pluginName + "_complete");
        completePublisher = vertx.eventBus().publisher(completeAddress);
    }

    @Override
    public void start(AsyncResultHandler<Void> startupHandler, Handler<MessageStream> messageStreamHandler) {

        inputPlugin.run(startupHandler::handle, duplexStream -> {
            InPipeline inPipeline = inOutParserFactory.createDefaultInPipeline(pluginName, duplexStream);
            final OutPipeline outPipeline = outInFilterFactory.createDefaultOutPipeline(pluginName, inputObj, duplexStream.getWriteStream());
            final MessageStream messageStream = new MessageStream(inPipeline, outPipeline);
            //This code has gotten a little confusing. Really it's here just for a hook to enable unit tests to know
            //when a stream is completely done.
            messageStream.writeCompleteHandler(writeCompleteFuture -> {
                final Handler<WriteCompleteFuture> writeCompleteHandler = duplexStream.getWriteCompleteHandler();
                if (writeCompleteHandler != null) {
                    final WriteCompleteFuture<Void> interceptFuture = WriteCompleteFuture.future(writeCompleteFuture.getPipelinePack());
                    interceptFuture.setHandler(commitDone -> {
                        if (commitDone.failed()) {
                            completePublisher.write("Commit failed");
                            writeCompleteFuture.fail(commitDone.cause());
                            return;
                        }
                        completePublisher.write("Ok");
                        writeCompleteFuture.complete();

                    });
                    writeCompleteHandler.handle(interceptFuture);
                }
            });

            messageStreamHandler.handle(messageStream);
        });
    }

    @Override
    public void stop(AsyncResultHandler<Void> stopHandler) {
        inputPlugin.stop(stopHandler::handle);
    }
}
