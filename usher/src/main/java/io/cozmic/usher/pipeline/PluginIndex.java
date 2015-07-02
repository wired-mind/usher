package io.cozmic.usher.pipeline;

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
    private ConcurrentMap<String, T> componentMappings = Maps.newConcurrentMap();

    public PluginIndex(Map<String, Map.Entry<T, JsonObject>> components, String componentType) {

        this.components = components;
        this.componentType = componentType;
    }

    public <P> void build(Map<String, Map.Entry<P, JsonObject>> plugins) {
        for (Map.Entry<String, Map.Entry<P, JsonObject>> pluginSpec : plugins.entrySet()) {
            final Map.Entry<P, JsonObject> plugin = pluginSpec.getValue();
            final String componentName = plugin.getValue().getString(componentType);
            final String pluginName = pluginSpec.getKey();

            if (componentName == null) {
                return;
            }

            if (!components.containsKey(componentName)) {
                throw new IllegalArgumentException("Invalid component name: " + componentName);
            }

            componentMappings.put(pluginName, components.get(componentName).getKey());
        }
    }

    public boolean exists(String pluginName) {
        return componentMappings.containsKey(pluginName);
    }


    public T get(String pluginName) {
        return componentMappings.get(pluginName);
    }
}
