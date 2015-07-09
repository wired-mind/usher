package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.PluginIndex;
import io.cozmic.usher.plugins.PluginLoader;
import io.cozmic.usher.plugins.core.NullDecoder;
import io.cozmic.usher.plugins.core.NullSplitter;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.buffer.Buffer;

/**
 * This class is a factory that creates new MessageParsers. Each new input is wrapped by a MessageParser.
 * The MessageParser's job is to apply the splitting and decoding logic. This factory provides the MessageParser
 * new instances of the Splitter and Decoder plugins because each stream needs to maintain state separately
 * for splitting and decoding.
 */
public class InPipelineFactoryImpl implements InPipelineFactory {
    private PluginIndex<SplitterPlugin> splitterIndex;
    private PluginIndex<DecoderPlugin> decoderIndex;

    public InPipelineFactoryImpl(PluginLoader pluginLoader) {

        splitterIndex = pluginLoader.getSplitterIndex();
        decoderIndex = pluginLoader.getDecoderIndex();

    }



    private SplitterPlugin createSplitter(String pluginName) {
        if (!splitterIndex.exists(pluginName)) {
            return new NullSplitter();
        }

        final SplitterPlugin splitterPlugin = splitterIndex.get(pluginName);
        return splitterPlugin.createNew();
    }

    private DecoderPlugin createDecoder(String pluginName) {
        if (!decoderIndex.exists(pluginName)) {
            return new NullDecoder();
        }

        final DecoderPlugin decoderPlugin = decoderIndex.get(pluginName);
        return decoderPlugin.createNew();
    }

    @Override
    public InPipeline createDefaultInPipeline(String pluginName, DuplexStream<Buffer, Buffer> duplexStream) {
        SplitterPlugin splitterPlugin = createSplitter(pluginName);
        DecoderPlugin decoderPlugin = createDecoder(pluginName);
        return new DefaultInPipeline(duplexStream, splitterPlugin, decoderPlugin);
    }


}
