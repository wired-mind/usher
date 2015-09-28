package io.cozmic.usher;


import com.codahale.metrics.*;
import com.typesafe.config.*;
import io.cozmic.usher.pipeline.PipelineVerticle;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
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

    public static final int DEFAULT_HEALTH_CHECK_PORT = 8080;
    static Logger logger = LoggerFactory.getLogger(Start.class.getName());

    public void start(final Future<Void> startedResult) {
        final JsonObject finalUsherConfig = buildUsherConfig();
        final JsonObject globalUsherConfig = finalUsherConfig.getJsonObject("usher", new JsonObject());

        vertx.executeBlocking(future -> {
            if (vertx.isMetricsEnabled()) {
                final JsonArray metricRegistries = globalUsherConfig.getJsonArray("metricRegistries");
                for (Object metricRegistry : metricRegistries) {
                    JsonObject metricRegistryObj = (JsonObject)metricRegistry;
                    final String registryName = metricRegistryObj.getString("name");
                    final String dataDogKey = metricRegistryObj.getString("dataDogKey");

                    logger.info(String.format("Enabling %s metrics", registryName));
                    final MetricRegistry registry = SharedMetricRegistries.getOrCreate(registryName);

                    if (dataDogKey != null) {
                        startDatadogReporter(registry, dataDogKey);
                    } else {
                        startConsoleReporter(registry);
                    }
                }
            }
            future.complete();
        }, asyncResult -> {



            final int pipelineInstances = Runtime.getRuntime().availableProcessors();
            final DeploymentOptions options = new DeploymentOptions();
            options.setInstances(globalUsherConfig.getInteger("pipelineInstances", pipelineInstances));
            options.setConfig(finalUsherConfig);


            // To start, we'll just always return 200. Later we can inspect backend servers for health too
            final Integer healthCheckPort = globalUsherConfig.getInteger("healthCheckPort", DEFAULT_HEALTH_CHECK_PORT);
            vertx.createHttpServer()
                    .requestHandler(request -> {
                        final HttpServerResponse response = request.response();
                        response.setStatusCode(200);
                        response.end();
                    })
                    .listen(healthCheckPort);

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
        // Allow overriding of application.conf as the main usher file.
        final Object overrideUsherConfigFile = config().remove("usherConfigFile");
        final String usherConfigFile = overrideUsherConfigFile != null ? (String)overrideUsherConfigFile : "application";
        final Config defaultConfig = ConfigFactory.parseResourcesAnySyntax(usherConfigFile);

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
            logger.info("Datadog reporting started.");
        } catch (IOException e) {
            logger.error("Could not configure data dog reporter. Right now datadog integration only works with EC2");
        }
    }
}


