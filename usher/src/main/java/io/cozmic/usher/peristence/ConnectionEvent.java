package io.cozmic.usher.peristence;

import io.cozmic.usherprotocols.core.Connection;

/**
 * Created by chuck on 9/15/14.
 */
public class ConnectionEvent {
    private Connection connection;

    public void setConnection(Connection value) {
        this.connection = value;
    }
    public Connection getConnection() {
        return connection;
    }

}
