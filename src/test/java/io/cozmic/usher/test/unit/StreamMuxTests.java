package io.cozmic.usher.test.unit;

import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.pipeline.MessageInjectorImpl;
import io.cozmic.usher.pipeline.StreamMuxImpl;
import io.cozmic.usher.streams.MessageStream;
import io.cozmic.usher.test.FakeFilter;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * Created by chuck on 8/19/15.
 */
@RunWith(VertxUnitRunner.class)
public class StreamMuxTests {
    Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();


    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void canRegisterStream(TestContext context) {

        final StreamMuxImpl streamMux = new StreamMuxImpl(vertx);
        final FakeFilter fakeFilter = new FakeFilter();
        final Async async = context.async();
        final MessageInjectorImpl messageInjector = new MessageInjectorImpl(streamMux);
        fakeFilter.run(messageInjector, asyncResult -> {
            final MessageStream messageStream = asyncResult.result();
            messageStream.setMessageMatcher(MessageMatcher.always());
            streamMux.addStream(messageStream, true);
            final PipelinePack pack = new PipelinePack("hi");
            streamMux.write(pack);

            context.assertEquals(pack, fakeFilter.getLastPipelinePack());
            async.complete();
        });

    }

    @Test
    public void canUnregisterStream(TestContext context) {

        final StreamMuxImpl streamMux = new StreamMuxImpl(vertx);
        final FakeFilter fakeFilter = new FakeFilter();
        final Async async = context.async();
        final MessageInjectorImpl messageInjector = new MessageInjectorImpl(streamMux);
        fakeFilter.run(messageInjector, asyncResult -> {
            final MessageStream messageStream = asyncResult.result();
            messageStream.setMessageMatcher(MessageMatcher.always());
            streamMux.addStream(messageStream, true);
            final PipelinePack pack = new PipelinePack("hi");
            streamMux.write(pack);

            context.assertEquals(pack, fakeFilter.getLastPipelinePack());

            streamMux.unregisterAllConsumers();
            async.complete();
        });

    }

    @Test
    public void canAcknowledgeWrites(TestContext context) {
        final StreamMuxImpl streamMux = new StreamMuxImpl(vertx);
        final FakeFilter fakeFilter = new FakeFilter();
        final Async async = context.async();
        final MessageInjectorImpl messageInjector = new MessageInjectorImpl(streamMux);
        fakeFilter.run(messageInjector, asyncResult -> {
            final MessageStream messageStream = asyncResult.result();
            messageStream.setMessageMatcher(MessageMatcher.always());
            streamMux.addStream(messageStream, true);
            final PipelinePack pack = new PipelinePack("hi");
            streamMux.write(pack, context.asyncAssertSuccess(v -> {
                context.assertEquals(pack, fakeFilter.getLastPipelinePack());

                streamMux.unregisterAllConsumers();
                async.complete();
            }));


        });
    }
}
