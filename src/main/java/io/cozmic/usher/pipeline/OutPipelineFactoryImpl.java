package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.PluginIndex;
import io.cozmic.usher.plugins.PluginLoader;
import io.cozmic.usher.plugins.core.AvroEncoder;
import io.cozmic.usher.plugins.core.GenericAvroEncoder;
import io.cozmic.usher.plugins.core.NullFrameEncoder;
import io.cozmic.usher.plugins.v1protocol.UsherV1FrameEncoder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class OutPipelineFactoryImpl implements OutPipelineFactory {


    private final PluginIndex<EncoderPlugin> encoderIndex;
    private PluginIndex<FrameEncoderPlugin> frameEncoderIndex;

    public OutPipelineFactoryImpl(PluginLoader pluginLoader) {
        encoderIndex = pluginLoader.getEncoderIndex();
        frameEncoderIndex = pluginLoader.getFrameEncoderIndex();
    }



    @Override
    public OutPipeline createDefaultOutPipeline(String pluginName, JsonObject config, MessageMatcher messageMatcher, WriteStream<Buffer> writeStream){
        EncoderPlugin encoderPlugin = createEncoder(pluginName);
        FrameEncoderPlugin frameEncoderPlugin = createFrameEncoder(pluginName);


        boolean isAvroEncoder = encoderPlugin instanceof AvroEncoder;
        final boolean isFrameEncoderNotExplicitlySet = frameEncoderPlugin instanceof NullFrameEncoder;

        boolean useFramingDefault = isAvroEncoder && isFrameEncoderNotExplicitlySet;
        if (isFrameEncoderNotExplicitlySet) {
            frameEncoderPlugin = new UsherV1FrameEncoder(); //For now we'll have a default frameEncoder
        }
        boolean useFraming = config.getBoolean("useFraming", useFramingDefault);

        frameEncoderPlugin = useFraming ? frameEncoderPlugin : new NullFrameEncoder();

        return new DefaultOutPipeline(writeStream, config, encoderPlugin, frameEncoderPlugin, messageMatcher);
    }

    private FrameEncoderPlugin createFrameEncoder(String pluginName) {
        final FrameEncoderPlugin frameEncoderPlugin = frameEncoderIndex.get(pluginName);
        return frameEncoderPlugin.createNew();
    }


    private EncoderPlugin createEncoder(String pluginName) {
        final EncoderPlugin encoderPlugin = encoderIndex.get(pluginName);
        return encoderPlugin.createNew();
    }
}
