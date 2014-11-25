package io.cozmic.usher.peristence;

import io.cozmic.usherprotocols.core.Request;

/**
 * Created by chuck on 9/15/14.
 */
public class RequestEvent {
    private Request request;

    public void setRequest(Request value) {
        this.request = value;
    }
    public Request getRequest() {
        return request;
    }

}
