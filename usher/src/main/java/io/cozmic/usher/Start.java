package io.cozmic.usher;


import com.codahale.metrics.*;
import io.cozmic.usher.pipeline.PipelineVerticle;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.MetricNameFormatter;
import org.coursera.metrics.datadog.transport.HttpTransport;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.coursera.metrics.datadog.DatadogReporter.Expansion.*;

/**
 * Created by chuck on 10/23/14.
 */
public class Start extends AbstractVerticle {

    static Logger logger = LoggerFactory.getLogger(Start.class.getName());

    public void start(final Future<Void> startedResult) {

        if (vertx.isMetricsEnabled()) {
            logger.info("Enabling metrics");
            final MetricRegistry usher = SharedMetricRegistries.getOrCreate("usher");

            final String datadogApiKey = System.getenv("DATADOG_API_KEY");
            if (datadogApiKey != null) {
                startDatadogReporter(usher, datadogApiKey);
            } else {
                startConsoleReporter(usher);
            }
        }

        final int pipelineInstances = Runtime.getRuntime().availableProcessors();
        final DeploymentOptions options = new DeploymentOptions();
        options.setInstances(pipelineInstances);
        options.setConfig(config());


        vertx.deployVerticle(PipelineVerticle.class.getName(), options, deployId -> {
            if (deployId.failed()) {
                final Throwable cause = deployId.cause();
                logger.error(cause.getMessage(), cause);
                startedResult.fail(deployId.cause());
                return;
            }
            logger.info("Finished launching pipeline " + deployId.result());
            startedResult.complete();

        });
    }

    public void startConsoleReporter(MetricRegistry usher) {
        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(usher)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        consoleReporter.start(5, TimeUnit.SECONDS);
    }

    public void startDatadogReporter(MetricRegistry usher, String datadogApiKey) {
        EnumSet<DatadogReporter.Expansion> expansions = EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99);
        HttpTransport httpTransport = new HttpTransport.Builder().withApiKey(datadogApiKey).build();
        DatadogReporter reporter;
        try {
            reporter = DatadogReporter.forRegistry(usher)
                    .withEC2Host()
                    .withTransport(httpTransport)
                    .withExpansions(expansions)
                    .build();
            reporter.start(10, TimeUnit.SECONDS);
        } catch (IOException e) {
            logger.error("Could not configure data dog reporter. Right now datadog integration only works with EC2");
        }
    }

    private static JsonObject buildOutput() {
        return new JsonObject().put("type", "TcpOutput").put("host", "www.cnn.com").put("port", 80);
    }

    private static JsonObject buildTokenSplitter(String delimiter) {
        return new JsonObject().put("type", "TokenSplitter").put("delimiter", delimiter);
    }

    private static JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500);
    }

}


