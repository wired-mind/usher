package io.cozmic.usher.pipeline;

import com.google.common.collect.Lists;
import io.cozmic.usher.core.*;
import io.cozmic.usher.plugins.tcp.TcpInput;
import io.cozmic.usher.plugins.tcp.TcpOutput;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by chuck on 6/25/15.
 */
public class PluginFactory {

    private final Vertx vertx;
    private final JsonObject config;
    private ArrayList<InputRunner> inputRunners = Lists.newArrayList();
    private ArrayList<OutputRunner> outputRunners = Lists.newArrayList();

    public PluginFactory(Vertx vertx, JsonObject config) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException {

        this.vertx = vertx;
        this.config = config;
        final PluginLoader pluginLoader = new PluginLoader(vertx, config);
        final MessageParserFactoryImpl messageParserFactory = new MessageParserFactoryImpl(pluginLoader);
        final MessageFilterFactoryImpl messageFilterFactory = new MessageFilterFactoryImpl(pluginLoader);

        final Map<String, Map.Entry<InputPlugin, JsonObject>> inputs = pluginLoader.getInputs();
        final Map<String, Map.Entry<OutputPlugin, JsonObject>> outputs = pluginLoader.getOutputs();

        for (Map.Entry<String, Map.Entry<InputPlugin, JsonObject>> inputSpec : inputs.entrySet()) {
            final Map.Entry<InputPlugin, JsonObject> input = inputSpec.getValue();
            final JsonObject pluginConfig = input.getValue();
            inputRunners.add(createInputRunner(inputSpec.getKey(), pluginConfig, messageParserFactory, messageFilterFactory));
        }

        for (Map.Entry<String, Map.Entry<OutputPlugin, JsonObject>> outputSpec : outputs.entrySet()) {
            final Map.Entry<OutputPlugin, JsonObject> output = outputSpec.getValue();
            final JsonObject pluginConfig = output.getValue();
            outputRunners.add(createOutputRunner(outputSpec.getKey(), pluginConfig, messageParserFactory, messageFilterFactory));
        }

    }

    public List<OutputPlugin> createOutputs(JsonArray outputs) {
        final ArrayList<OutputPlugin> outputPlugins = Lists.newArrayList();
        for (Object output : outputs) {
            outputPlugins.add(createOutput((JsonObject) output));
        }
        return outputPlugins;
    }

    public List<InputRunner> getInputRunners() {
        return inputRunners;
    }

    public static Plugin create(JsonObject inputObj) {
        return null;
    }

    public InputPlugin createInput(JsonObject inputObj) {

        final TcpInput tcpInput = new TcpInput();
        tcpInput.init(inputObj, vertx);
        return tcpInput;
    }



    public InputRunner createInputRunner(String pluginName, JsonObject inputObj, MessageParserFactoryImpl inOutParserFactory, MessageFilterFactoryImpl outInFilterFactory) {
        InputPlugin inputPlugin = createInput(inputObj);
        final String expressionVal = inputObj.getString("messageMatcher");
        final MessageMatcher messageMatcher = expressionVal == null ? MessageMatcher.always() : new JuelMatcher(expressionVal);
        return new InputRunnerImpl(pluginName, inputPlugin, messageMatcher, inOutParserFactory, outInFilterFactory);
    }

    public OutputPlugin createOutput(JsonObject outputObj) {
        final TcpOutput tcpOutput = new TcpOutput();
        tcpOutput.init(outputObj, vertx);
        return tcpOutput;
    }

    public OutputRunner createOutputRunner(String pluginName, JsonObject outputObj, MessageParserFactoryImpl outInParserFactory, MessageFilterFactoryImpl inOutFilterFactory) {
        OutputPlugin outputPlugin = createOutput(outputObj);
        final String expressionVal = outputObj.getString("messageMatcher");
        final MessageMatcher messageMatcher = expressionVal == null ? MessageMatcher.never() : new JuelMatcher(expressionVal);
        return new OutputRunnerImpl(pluginName, outputPlugin, messageMatcher, outInParserFactory, inOutFilterFactory);
    }

    public List<OutputRunner> getOutputRunners() {
        return outputRunners;
    }
}
