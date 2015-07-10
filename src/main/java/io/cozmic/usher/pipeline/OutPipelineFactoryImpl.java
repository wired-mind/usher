package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutPipelineFactory;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.plugins.PluginIndex;
import io.cozmic.usher.plugins.PluginLoader;
import io.cozmic.usher.plugins.core.NullEncoder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class OutPipelineFactoryImpl implements OutPipelineFactory {


    private final PluginIndex<EncoderPlugin> encoderIndex;

    public OutPipelineFactoryImpl(PluginLoader pluginLoader) {
        encoderIndex = pluginLoader.getEncoderIndex();
    }



    @Override
    public OutPipeline createDefaultOutPipeline(String pluginName, MessageMatcher messageMatcher, WriteStream<Buffer> writeStream){
        EncoderPlugin encoderPlugin = createEncoder(pluginName);

        return new DefaultOutPipeline(writeStream, encoderPlugin, messageMatcher);
    }



    private EncoderPlugin createEncoder(String pluginName) {
        if (!encoderIndex.exists(pluginName)) {
            return new NullEncoder();
        }

        final EncoderPlugin encoderPlugin = encoderIndex.get(pluginName);
        return encoderPlugin.createNew();
    }
}
