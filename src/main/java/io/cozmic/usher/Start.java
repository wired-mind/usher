package io.cozmic.usher;


import com.codahale.metrics.*;
import com.typesafe.config.*;
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
        final JsonObject finalUsherConfig = buildUsherConfig();

        final int pipelineInstances = Runtime.getRuntime().availableProcessors();
        final DeploymentOptions options = new DeploymentOptions();
        options.setInstances(pipelineInstances);
        options.setConfig(finalUsherConfig);


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

    /**
     * Provides a "Convention over Configuration" approach to config files for usher.
     *
     * Services built with usher can put an application.conf file in the classpath and call it good.
     * However, they can also use the USHER_ENV environment variable to specify a runtime environment
     * and load/merge values from other config files.
     * @return
     */
    private JsonObject buildUsherConfig() {
        //https://github.com/typesafehub/config#standard-behavior
        final Config refConfig = ConfigFactory.parseResourcesAnySyntax("reference");
        final Config defaultConfig = ConfigFactory.parseResourcesAnySyntax("application");

        //load a production.conf if any
        String env = System.getenv("USHER_ENV");
        if (env == null) {
            env = "production";
        }
        final Config envConfig = ConfigFactory.parseResourcesAnySyntax(String.format("%s.conf", env));

        JsonObject runtimeConfig = config();
        if (runtimeConfig == null) {
            runtimeConfig = new JsonObject();
        }

        final Config runtimeOverrides = ConfigFactory.parseString(runtimeConfig.toString(), ConfigParseOptions.defaults());
        Config resolvedConfigs;
        resolvedConfigs = runtimeOverrides
                .withFallback(envConfig)
                .withFallback(defaultConfig)
                .withFallback(refConfig)
                .resolve();


        return new JsonObject(resolvedConfigs.root().render(ConfigRenderOptions.concise()));
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
}


