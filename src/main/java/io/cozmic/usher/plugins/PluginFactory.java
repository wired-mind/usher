package io.cozmic.usher.plugins;

import com.google.common.collect.Lists;
import io.cozmic.usher.core.*;
import io.cozmic.usher.pipeline.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * Created by chuck on 6/25/15.
 */
public class PluginFactory {

    private final Vertx vertx;
    private final JsonObject config;
    private List<InputRunner> inputRunners = Lists.newArrayList();
    private List<OutputRunner> outputRunners = Lists.newArrayList();
    private List<FilterRunner> filterRunners = Lists.newArrayList();

    public PluginFactory(Vertx vertx, JsonObject config) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {

        this.vertx = vertx;
        this.config = config;
        final PluginLoader pluginLoader = new PluginLoader(vertx, config);
        final InPipelineFactoryImpl messageParserFactory = new InPipelineFactoryImpl(pluginLoader);
        final OutPipelineFactoryImpl messageFilterFactory = new OutPipelineFactoryImpl(pluginLoader);

        final Map<String, Map.Entry<InputPlugin, JsonObject>> inputs = pluginLoader.getInputs();
        final Map<String, Map.Entry<OutputPlugin, JsonObject>> outputs = pluginLoader.getOutputs();
        final Map<String, Map.Entry<FilterPlugin, JsonObject>> filters = pluginLoader.getFilters();

        for (Map.Entry<String, Map.Entry<InputPlugin, JsonObject>> inputSpec : inputs.entrySet()) {
            final Map.Entry<InputPlugin, JsonObject> input = inputSpec.getValue();
            inputRunners.add(createInputRunner(inputSpec.getKey(), input.getValue(), input.getKey(), messageParserFactory, messageFilterFactory));
        }

        for (Map.Entry<String, Map.Entry<OutputPlugin, JsonObject>> outputSpec : outputs.entrySet()) {
            final Map.Entry<OutputPlugin, JsonObject> output = outputSpec.getValue();
            outputRunners.add(createOutputRunner(outputSpec.getKey(), output.getValue(), output.getKey(), messageParserFactory, messageFilterFactory));
        }

        for (Map.Entry<String, Map.Entry<FilterPlugin, JsonObject>> filterSpec : filters.entrySet()) {
            final Map.Entry<FilterPlugin, JsonObject> filter = filterSpec.getValue();
            filterRunners.add(createFilterRunner(filterSpec.getKey(), filter.getValue(), filter.getKey()));
        }
    }



    public InputRunner createInputRunner(String pluginName, JsonObject inputObj, InputPlugin inputPlugin, InPipelineFactoryImpl inOutParserFactory, OutPipelineFactoryImpl outInFilterFactory) {
        final String expressionVal = inputObj.getString("messageMatcher");
        final MessageMatcher messageMatcher = expressionVal == null ? MessageMatcher.always() : new JuelMatcher(expressionVal);
        return new InputRunnerImpl(pluginName, inputObj, inputPlugin, messageMatcher, inOutParserFactory, outInFilterFactory);
    }



    public OutputRunner createOutputRunner(String pluginName, JsonObject outputObj, OutputPlugin outputPlugin, InPipelineFactoryImpl outInParserFactory, OutPipelineFactoryImpl inOutFilterFactory) {
        final String expressionVal = outputObj.getString("messageMatcher");
        final MessageMatcher messageMatcher = expressionVal == null ? MessageMatcher.never() : new JuelMatcher(expressionVal);
        return new OutputRunnerImpl(pluginName, outputObj, outputPlugin, messageMatcher, outInParserFactory, inOutFilterFactory);
    }


    private FilterRunner createFilterRunner(String pluginName, JsonObject filterObj, FilterPlugin filterPlugin) {
        final String expressionVal = filterObj.getString("messageMatcher");
        final MessageMatcher messageMatcher = expressionVal == null ? MessageMatcher.never() : new JuelMatcher(expressionVal);
        return new FilterRunnerImpl(pluginName, filterPlugin, messageMatcher);
    }

    public List<InputRunner> getInputRunners() {
        return inputRunners;
    }

    public List<OutputRunner> getOutputRunners() {
        return outputRunners;
    }

    public List<FilterRunner> getFilterRunners() {
        return filterRunners;
    }
}
