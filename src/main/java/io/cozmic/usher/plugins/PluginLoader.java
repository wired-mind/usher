package io.cozmic.usher.plugins;

import com.google.common.collect.Maps;
import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.core.*;
import io.cozmic.usher.plugins.v1protocol.UsherV1FrameEncoder;
import io.cozmic.usher.plugins.v1protocol.UsherV1FramingSplitter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by chuck on 7/1/15.
 */
public class PluginLoader {
    private final PluginIndex<DecoderPlugin> decoderIndex;
    private PluginIndex<SplitterPlugin> splitterIndex;
    private PluginIndex<EncoderPlugin> encoderIndex;
    private Map<String, Map.Entry<InputPlugin, JsonObject>> inputPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<SplitterPlugin, JsonObject>> splitterPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<DecoderPlugin, JsonObject>> decoderPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<OutputPlugin, JsonObject>> outputPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<EncoderPlugin, JsonObject>> encoderPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<FrameEncoderPlugin, JsonObject>> frameEncoderPlugins = Maps.newConcurrentMap();
    private Map<String, Map.Entry<FilterPlugin, JsonObject>> filterPlugins = Maps.newConcurrentMap();
    private Map<String, String> wellKnownPackages;
    private PluginIndex<FrameEncoderPlugin> frameEncoderIndex;


    private Properties loadTypePackages() throws IOException {
        final InputStream stream = getClass().getClassLoader().getResourceAsStream("typePackages.properties");
        if (stream == null) {
            throw new FileNotFoundException("Could not find typePackages.properties file for PlugingLoader");
        }
        final Properties properties = new Properties();
        properties.load(stream);
        return properties;
    }


    public PluginLoader(Vertx vertx, JsonObject config) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException {
        wellKnownPackages = Maps.fromProperties(loadTypePackages());
        splitterPlugins.put("NullSplitter", Maps.immutableEntry((SplitterPlugin) new NullSplitter(), new JsonObject()));
        splitterPlugins.put("UsherV1FramingSplitter", Maps.immutableEntry((SplitterPlugin) new UsherV1FramingSplitter(), new JsonObject()));
        decoderPlugins.put("NullDecoder", Maps.immutableEntry((DecoderPlugin) new NullDecoder(), new JsonObject()));
        encoderPlugins.put("NullEncoder", Maps.immutableEntry((EncoderPlugin) new NullEncoder(), new JsonObject()));
        frameEncoderPlugins.put("NullFrameEncoder", Maps.immutableEntry((FrameEncoderPlugin) new NullFrameEncoder(), new JsonObject()));
        frameEncoderPlugins.put("UsherV1FrameEncoder", Maps.immutableEntry((FrameEncoderPlugin) new UsherV1FrameEncoder(), new JsonObject()));

        for (String pluginName : config.fieldNames()) {
            final JsonObject pluginConfig = config.getJsonObject(pluginName);
            String pluginType = pluginConfig.getString("type", pluginName);
            final boolean noPackage = !pluginType.contains(".");
            if (noPackage) {
                pluginType = enrichPluginTypeWithWellKnownPackages(pluginType);
            }
            final Class<?> pluginClass = Class.forName(pluginType);
            final Constructor<?> constructor = pluginClass.getConstructor();


            final Plugin plugin = (Plugin) constructor.newInstance();
            plugin.init(pluginConfig, vertx);

            if (pluginType.endsWith("Input")) {
                inputPlugins.put(pluginName, Maps.immutableEntry((InputPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Output")) {
                outputPlugins.put(pluginName, Maps.immutableEntry((OutputPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Splitter")) {
                splitterPlugins.put(pluginName, Maps.immutableEntry((SplitterPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Decoder")) {
                decoderPlugins.put(pluginName, Maps.immutableEntry((DecoderPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Encoder")) {
                encoderPlugins.put(pluginName, Maps.immutableEntry((EncoderPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Filter")) {
                filterPlugins.put(pluginName, Maps.immutableEntry((FilterPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("FrameEncoder")) {
                frameEncoderPlugins.put(pluginName, Maps.immutableEntry((FrameEncoderPlugin) plugin, pluginConfig));
            }
        }

        splitterIndex = new PluginIndex<>(getSplitters(), "splitter", "NullSplitter");
        splitterIndex.build(getInputs());
        splitterIndex.build(getOutputs());

        decoderIndex = new PluginIndex<>(getDecoders(), "decoder", "NullDecoder");
        decoderIndex.build(getInputs());
        decoderIndex.build(getOutputs());

        encoderIndex = new PluginIndex<>(getEncoders(), "encoder", "NullEncoder");
        encoderIndex.build(getInputs());
        encoderIndex.build(getOutputs());


        frameEncoderIndex = new PluginIndex<>(getFrameEncoders(), "frameEncoder", "NullFrameEncoder");
        frameEncoderIndex.build(getInputs());
        frameEncoderIndex.build(getOutputs());
    }





    private String enrichPluginTypeWithWellKnownPackages(String pluginType) {
        return String.format("%s.%s", wellKnownPackages.get(pluginType), pluginType);
    }

    public Map<String, Map.Entry<SplitterPlugin, JsonObject>> getSplitters() {
        return splitterPlugins;
    }

    public Map<String, Map.Entry<InputPlugin, JsonObject>> getInputs() {
        return inputPlugins;
    }

    public Map<String, Map.Entry<OutputPlugin, JsonObject>> getOutputs() {
        return outputPlugins;
    }

    public Map<String, Map.Entry<EncoderPlugin, JsonObject>> getEncoders() {
        return encoderPlugins;
    }

    public Map<String, Map.Entry<DecoderPlugin, JsonObject>> getDecoders() {
        return decoderPlugins;
    }

    public Map<String, Map.Entry<FilterPlugin, JsonObject>> getFilters() {
        return filterPlugins;
    }

    public Map<String,Map.Entry<FrameEncoderPlugin,JsonObject>> getFrameEncoders() {
        return frameEncoderPlugins;
    }



    public PluginIndex<SplitterPlugin> getSplitterIndex() {
        return splitterIndex;
    }

    public PluginIndex<DecoderPlugin> getDecoderIndex() {
        return decoderIndex;
    }

    public PluginIndex<EncoderPlugin> getEncoderIndex() {
        return encoderIndex;
    }

    public PluginIndex<FrameEncoderPlugin> getFrameEncoderIndex() {
        return frameEncoderIndex;
    }
}
