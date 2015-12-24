package io.cozmic.usher.plugins.kafka;

/**
 * A (host, port) pair.
 * <p>
 * Created by Craig Earley on 12/23/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class HostAndPort extends Tuple<String, Integer> {

    public HostAndPort(String host, Integer port) {
        super(host, port);
    }

    public String getHost() {
        return _1;
    }

    public Integer getPort() {
        return _2;
    }
}
