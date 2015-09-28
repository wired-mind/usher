package io.cozmic.usher.test.integration;


import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.fail;

/**
 * File plugin is a work in progress. Putting in just enough now to write debug logs
 */
@RunWith(VertxUnitRunner.class)
public class LogOutputTests {

    Vertx vertx;
    private LogHandler fakeLogHandler;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx(new VertxOptions());
        Logger testLogger = Logger.getLogger("test_logger");

        fakeLogHandler = new LogHandler();
        testLogger.addHandler(fakeLogHandler);
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    /**
     * Log plugin is rudimentary for now. It will return a single byte to indicate the Write is done
     * @param context
     */
    @Test
    public void testCanWriteToLog(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            final NetClient netClient = vertx.createNetClient();
            netClient.connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.handler(buffer -> {
                    context.assertEquals((byte) 0x1, buffer.getByte(0));
                    context.assertEquals(Hex.encodeHexString("Hello Log".getBytes()), fakeLogHandler.lastRecord.getMessage());
                    async.complete();
                });

                socket.write("Hello Log");
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
            final URI uri = getClass().getResource("/config_log_output.json").toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }


    class LogHandler extends java.util.logging.Handler  {
        Level lastLevel = Level.FINEST;
        private LogRecord lastRecord;

        public Level  checkLevel() {
            return lastLevel;
        }



        public void close(){}

        @Override
        public void publish(LogRecord record) {

            lastRecord = record;
        }

        public void flush(){}


    }

}
