package io.cozmic.usher.streams;

import com.google.common.collect.Lists;
import io.cozmic.usher.core.*;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Created by chuck on 7/6/15.
 */
public class ChannelFactoryImpl implements ChannelFactory {
    Logger logger = LoggerFactory.getLogger(ChannelFactoryImpl.class.getName());
    private final ObjectPool<StreamDemultiplexer> outStreamDemultiplexerPool;

    public ChannelFactoryImpl(ObjectPool<StreamDemultiplexer> outStreamDemultiplexerPool) {
        this.outStreamDemultiplexerPool = outStreamDemultiplexerPool;
    }


    public void create(MessageParser inMessageParser, AsyncResultHandler<Channel> handler) {
        create(inMessageParser, null, handler);
    }

    public void create(MessageParser inMessageParser, MessageStream optionalMessageStream, AsyncResultHandler<Channel> handler) {
        Channel channel = new ChannelImpl(inMessageParser, outStreamDemultiplexerPool);
        channel.init(optionalMessageStream, handler);
    }

    @Override
    public void createDuplexChannel(MessageStream inputMessageStream) {
        create(inputMessageStream.getMessageParser(), inputAsyncResult -> {
            final Channel inputChannel = inputAsyncResult.result();
            ensureChannel(inputChannel);

            buildOutputChannels(inputMessageStream, inputChannel.getResponseStreams(), outputAsyncResult -> {
                final List<Channel> outputChannels = outputAsyncResult.result();
                for (Channel outputChannel : outputChannels) {
                    outputChannel.start();
                }
                inputChannel.start().endHandler(v -> {
                    for (Channel outputChannel : outputChannels) {
                        outputChannel.stop();
                    }
                });
            });
        });
    }



    private void buildOutputChannels(MessageStream inputMessageStream, List<MessageParser> responseStreams, AsyncResultHandler<List<Channel>> handler) {
        List<Channel> outputChannels = Lists.newArrayList();
        final CountDownFutureResult<Void> result = new CountDownFutureResult<>(responseStreams.size());
        for (MessageParser responseStream : responseStreams) {
            //TODO: clone/wrap this
            create(responseStream, inputMessageStream, asyncResult -> {
                final Channel channel = asyncResult.result();
                ensureChannel(channel);
                outputChannels.add(channel);
                result.complete();
            });
        }

        result.setHandler(v-> handler.handle(Future.succeededFuture(outputChannels)));
    }

    private void ensureChannel(Channel inputChannel) {
        Objects.requireNonNull(inputChannel, "Channel factory should never return with errors. A channel should ensure full initialization before returning.");
    }
}
