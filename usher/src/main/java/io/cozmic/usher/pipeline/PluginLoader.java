package io.cozmic.usher.pipeline;

import com.google.common.collect.Maps;
import io.cozmic.usher.core.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

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
    private Map<String, String> wellKnownPackages;


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
        for (String pluginName : config.fieldNames()) {
            final JsonObject pluginConfig = config.getJsonObject(pluginName);
            String pluginType = pluginConfig.getString("type");
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
            }
            else if (pluginType.endsWith("Output")) {
                outputPlugins.put(pluginName, Maps.immutableEntry((OutputPlugin) plugin, pluginConfig));
            }
            else if (pluginType.endsWith("Splitter")) {
                splitterPlugins.put(pluginName, Maps.immutableEntry((SplitterPlugin) plugin, pluginConfig));
            }
            else if (pluginType.endsWith("Decoder")) {
                decoderPlugins.put(pluginName, Maps.immutableEntry((DecoderPlugin) plugin, pluginConfig));
            }
            else if (pluginType.endsWith("Encoder")) {
                encoderPlugins.put(pluginName, Maps.immutableEntry((EncoderPlugin) plugin, pluginConfig));
            }
        }

        splitterIndex = new PluginIndex<>(getSplitters(), "splitter");
        splitterIndex.build(getInputs());
        splitterIndex.build(getOutputs());

        decoderIndex = new PluginIndex<>(getDecoders(), "decoder");
        decoderIndex.build(getInputs());
        decoderIndex.build(getOutputs());

        encoderIndex = new PluginIndex<>(getEncoders(), "encoder");
        encoderIndex.build(getInputs());
        encoderIndex.build(getOutputs());

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

    public PluginIndex<SplitterPlugin> getSplitterIndex() {
        return splitterIndex;
    }

    public PluginIndex<DecoderPlugin> getDecoderIndex() {
        return decoderIndex;
    }

    public PluginIndex<EncoderPlugin> getEncoderIndex() {
        return encoderIndex;
    }
}
