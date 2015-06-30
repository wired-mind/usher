package io.cozmic.usher;


import io.cozmic.usher.core.CountDownFutureResult;
import io.cozmic.usher.pipeline.PipelineVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by chuck on 10/23/14.
 */
public class Start extends AbstractVerticle {

    Logger logger = LoggerFactory.getLogger(Start.class.getName());

    public void start(final Future<Void> startedResult) {
        final int pipelineInstances = Runtime.getRuntime().availableProcessors();
        final DeploymentOptions options = new DeploymentOptions();
        options.setInstances(pipelineInstances);
        options.setConfig(config());


        vertx.deployVerticle(PipelineVerticle.class.getName(), options, deployId -> {
            if (deployId.failed()) {
                startedResult.fail(deployId.cause());
                return;
            }
            logger.info("Finished launching pipeline " + deployId.result());
            startedResult.complete();
        });


    }
}


