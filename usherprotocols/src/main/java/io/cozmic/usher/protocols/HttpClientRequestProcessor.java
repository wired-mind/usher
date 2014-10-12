package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.MessageProcessor;
import org.vertx.java.core.Handler;

/**
 * Created by chuck on 9/17/14.
 */
public class HttpClientRequestProcessor implements MessageProcessor {
    private Handler<Message> output;

    public HttpClientRequestProcessor() {

    }

    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }

    @Override
    public void handle(Message message) {
        // add header? - proxy design doesn't maintain a standard req/rep httpClient pool. Instead
        // we'll modify the message and the app server will respond with modification in tact

        //TODO: http header modification
        getOutput().handle(message);
    }


    private Handler<Message> getOutput() {
        return output;
    }
}
