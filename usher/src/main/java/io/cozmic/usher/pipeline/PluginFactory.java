package io.cozmic.usher.pipeline;

import com.google.common.collect.Lists;
import io.cozmic.usher.plugins.tcp.TcpInput;
import io.cozmic.usher.plugins.tcp.TcpOutput;
import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.core.Plugin;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuck on 6/25/15.
 */
public class PluginFactory {

    private final Vertx vertx;

    public PluginFactory(Vertx vertx) {

        this.vertx = vertx;
    }

    public List<OutputPlugin> createOutputs(JsonArray outputs) {
        final ArrayList<OutputPlugin> outputPlugins = Lists.newArrayList();
        for (Object output : outputs) {
            outputPlugins.add(createOutput((JsonObject) output));
        }
        return outputPlugins;
    }

    public List<InputRunner> createInputRunners(JsonArray inputs) {
        final ArrayList<InputRunner> inputRunners = Lists.newArrayList();

        for (Object input : inputs) {
            inputRunners.add(createInputRunner((JsonObject) input));
        }
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



    public InputRunner createInputRunner(JsonObject inputObj) {

        InputPlugin inputPlugin = createInput(inputObj);
        return new InputRunnerImpl(inputPlugin, new MessageParserFactoryImpl(inputObj));
    }

    public OutputPlugin createOutput(JsonObject outputObj) {
        final TcpOutput tcpOutput = new TcpOutput();
        tcpOutput.init(outputObj, vertx);
        return tcpOutput;
    }

    public OutputRunner createOutputRunner(JsonObject outputObj) {
        OutputPlugin outputPlugin = createOutput(outputObj);
        return new OutputRunnerImpl(outputPlugin, new MessageFilterFactoryImpl(outputObj));
    }

    public List<OutputRunner> createOutputRunners(JsonArray outputs) {
        final ArrayList<OutputRunner> outputRunners = Lists.newArrayList();

        for (Object output : outputs) {
            outputRunners.add(createOutputRunner((JsonObject) output));
        }
        return outputRunners;
    }
}
