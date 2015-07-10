package io.cozmic.usher.plugins;

import com.google.common.collect.Maps;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores a mapping of plugin name and instances of components, e.g. Splitter, Decoder, Encoder
 */
public class PluginIndex<T> {
    private final Map<String, Map.Entry<T, JsonObject>> components;
    private String componentType;
    private final String defaultType;
    private ConcurrentMap<String, Map.Entry<T, JsonObject>> componentMappings = Maps.newConcurrentMap();

    public PluginIndex(Map<String, Map.Entry<T, JsonObject>> components, String componentType, String defaultType) {

        this.components = components;
        this.componentType = componentType;
        this.defaultType = defaultType;
    }

    public <P> void build(Map<String, Map.Entry<P, JsonObject>> plugins) {
        for (Map.Entry<String, Map.Entry<P, JsonObject>> pluginSpec : plugins.entrySet()) {
            final Map.Entry<P, JsonObject> plugin = pluginSpec.getValue();
            final String componentName = plugin.getValue().getString(componentType, defaultType);
            final String pluginName = pluginSpec.getKey();


            if (!components.containsKey(componentName)) {
                throw new IllegalArgumentException("Invalid component name: " + componentName);
            }

            componentMappings.put(pluginName, components.get(componentName));
        }
    }

    public boolean exists(String pluginName) {
        return componentMappings.containsKey(pluginName);
    }


    public T get(String pluginName) {
        return componentMappings.get(pluginName).getKey();
    }


    public JsonObject getConfig(String pluginName) {
        return componentMappings.get(pluginName).getValue();
    }
}
