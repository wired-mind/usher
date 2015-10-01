package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
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

    public InputRunnerImpl(String pluginName, JsonObject inputObj, InputPlugin inputPlugin, MessageMatcher messageMatcher, InPipelineFactory inOutParserFactory, OutPipelineFactoryImpl outInFilterFactory) {
        this.pluginName = pluginName;
        this.inputObj = inputObj;

        this.inputPlugin = inputPlugin;
        this.messageMatcher = messageMatcher;
        this.inOutParserFactory = inOutParserFactory;
        this.outInFilterFactory = outInFilterFactory;


    }

    @Override
    public void start(AsyncResultHandler<Void> startupHandler, Handler<MessageStream> messageStreamHandler) {

        inputPlugin.run(startupHandler::handle, duplexStream -> {
            InPipeline inPipeline = inOutParserFactory.createDefaultInPipeline(pluginName, duplexStream);
            final OutPipeline outPipeline = outInFilterFactory.createDefaultOutPipeline(pluginName, inputObj, duplexStream.getWriteStream());
            messageStreamHandler.handle(new MessageStream(inPipeline, outPipeline));
        });
    }

    @Override
    public void stop(AsyncResultHandler<Void> stopHandler) {
        inputPlugin.stop(stopHandler::handle);
    }
}
