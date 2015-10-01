package io.cozmic.usher.test.integration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.cozmic.usher.Start;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.core.OutputRunner;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.pipeline.*;
import io.cozmic.usher.plugins.PluginLoader;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Created by chuck on 10/1/15.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaOutputTests {
    Vertx vertx;


    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        vertx.setTimer(5000, event -> context.fail("timed out"));

    }

    @After
    public void after(TestContext context) {

        vertx.close(context.asyncAssertSuccess());
    }


    private JsonObject getReferenceConfig() {
        final Config refConfig = ConfigFactory.parseResourcesAnySyntax("reference");
        return new JsonObject(refConfig.resolve().root().render(ConfigRenderOptions.concise()));
    }


    @Test
    public void testCanPublishToKafka(TestContext context) throws NoSuchMethodException, IllegalAccessException, InstantiationException, UsherInitializationFailedException, InvocationTargetException, IOException {
        final StreamMuxImpl streamMux = new StreamMuxImpl(vertx);

        final MessageMatcher messageMatcher = MessageMatcher.always();


        final JsonObject config = getReferenceConfig();
        config.put("KafkaOutput", new JsonObject());
        final OutputRunnerFactory outputRunnerFactory = new OutputRunnerFactory(vertx, config);
        final OutputRunner outputRunner = outputRunnerFactory.buildOutputRunner(messageMatcher, "KafkaOutput");
        outputRunner.run(context.asyncAssertSuccess(messageStream -> {
            streamMux.addStream(messageStream, true);

            final PipelinePack pack = new PipelinePack();
            pack.setMessage(Buffer.buffer("hi"));
            final Async async = context.async();
            streamMux.write(pack, context.asyncAssertSuccess(v -> {
                streamMux.unregisterAllConsumers();
                async.complete();
            }));
        }));


    }


    @Test
    public void testCanTraverseKakfa(TestContext context) {
        final String expectedMessage = "Hello";
        final DeploymentOptions options = buildDeploymentOptions("/config_kafka_output.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();

                vertx.eventBus().<Integer>consumer(EventBusFilter.EVENT_BUS_ADDRESS, msg -> {
                    final Integer hashCode = msg.body();
                    context.assertEquals(expectedMessage.hashCode(), hashCode);
                    async.complete();
                });

                socket.write(expectedMessage);
            });

            vertx.setTimer(15000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });

        }));
    }



    private class OutputRunnerFactory {
        private PluginLoader pluginLoader;

        public OutputRunnerFactory(Vertx vertx, JsonObject config) throws UsherInitializationFailedException {
            pluginLoader = new PluginLoader(vertx, config);
        }
        /**
         * This might become a handy function. If so we'll extract.
         * @param messageMatcher
         * @param pluginName
         * @return
         * @throws UsherInitializationFailedException
         */
        private OutputRunner buildOutputRunner(MessageMatcher messageMatcher, String pluginName) throws UsherInitializationFailedException {


            final InPipelineFactoryImpl inPipelineFactory = new InPipelineFactoryImpl(pluginLoader);
            final OutPipelineFactoryImpl outPipelineFactory = new OutPipelineFactoryImpl(pluginLoader);
            final Map.Entry<OutputPlugin, JsonObject> pluginEntry = pluginLoader.getOutputs().get(pluginName);
            final OutputRunnerImpl outputRunner = new OutputRunnerImpl(pluginName, pluginEntry.getValue(), pluginEntry.getKey(), messageMatcher, inPipelineFactory, outPipelineFactory);
            return outputRunner;
        }

        public PluginLoader getPluginLoader() {
            return pluginLoader;
        }
    }
    public DeploymentOptions buildDeploymentOptions(String configFile) {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource(configFile).toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }
}
