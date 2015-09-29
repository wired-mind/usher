package io.cozmic.usher.plugins;

import com.google.common.collect.Maps;
import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by chuck on 7/1/15.
 */
public class PluginLoader {
    Logger logger = LoggerFactory.getLogger(PluginLoader.class.getName());

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


    public PluginLoader(Vertx vertx, JsonObject config) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException, UsherInitializationFailedException {
        wellKnownPackages = Maps.fromProperties(loadTypePackages());

        final Set<String> pluginNames = config.fieldNames();
        pluginNames.remove("usher");
        for (String pluginName : pluginNames) {
            final Object configObj = config.getValue(pluginName);
            if (!(configObj instanceof JsonObject)) {
                logger.warn(String.format("[Usher] - Ignoring %s. Does not appear to be an usher plugin.", pluginName));
                continue;
            }
            final JsonObject pluginConfig = (JsonObject)configObj;
            String pluginType = pluginConfig.getString("type", pluginName);
            final boolean noPackage = !pluginType.contains(".");
            if (noPackage) {
                pluginType = enrichPluginTypeWithWellKnownPackages(pluginType);
            }


            Plugin plugin;
            try {
                final Class<?> pluginClass = Class.forName(pluginType);
                final Constructor<?>constructor = pluginClass.getConstructor();
                plugin = (Plugin) constructor.newInstance();
            } catch (ClassNotFoundException e) {
                logger.warn(String.format("[Usher] - Ignoring %s. Does not appear to be an usher plugin.", pluginName));
                continue;
            }

            plugin.init(pluginConfig, vertx);

            if (pluginType.endsWith("Input")) {
                inputPlugins.put(pluginName, Maps.immutableEntry((InputPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Output")) {
                outputPlugins.put(pluginName, Maps.immutableEntry((OutputPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Splitter")) {
                splitterPlugins.put(pluginName, Maps.immutableEntry((SplitterPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Decoder")) {
                decoderPlugins.put(pluginName, Maps.immutableEntry((DecoderPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("FrameEncoder")) {
                frameEncoderPlugins.put(pluginName, Maps.immutableEntry((FrameEncoderPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Encoder")) {
                encoderPlugins.put(pluginName, Maps.immutableEntry((EncoderPlugin) plugin, pluginConfig));
            } else if (pluginType.endsWith("Filter")) {
                filterPlugins.put(pluginName, Maps.immutableEntry((FilterPlugin) plugin, pluginConfig));
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
