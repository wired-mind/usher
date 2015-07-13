package io.cozmic.usher.test.integration;


import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

/**
 * File plugin is a work in progress. Putting in just enough now to write debug logs
 */
@RunWith(VertxUnitRunner.class)
public class FileOutputTests {

    Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true)));
        final FileSystem fileSystem = vertx.fileSystem();
        if (fileSystem.existsBlocking("/tmp/debug_log")) {
            fileSystem.deleteBlocking("/tmp/debug_log");
        }
    }

    @After
    public void after(TestContext context) {
        final FileSystem fileSystem = vertx.fileSystem();
        if (fileSystem.existsBlocking("/tmp/debug_log")) {
            fileSystem.deleteBlocking("/tmp/debug_log");
        }
        vertx.close(context.asyncAssertSuccess());
    }


    /**
     * File plugin is rudimentary for now. It will return a single byte to indicate the Write is done
     * @param context
     */
    @Test
    public void testFileCanWrite(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("Hello File");
                socket.handler(buffer -> {
                    context.assertEquals((byte)0x1, buffer.getByte(0));
                    final Buffer fileContents = vertx.fileSystem().readFileBlocking("/tmp/debug_log");
                    context.assertEquals("Hello File", fileContents.toString());
                    async.complete();
                });
            });

            vertx.setTimer(5000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });

        }));
    }

    public DeploymentOptions buildDeploymentOptions() {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource("/config_file_output.json").toURI();
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
