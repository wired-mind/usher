package io.cozmic.usher.plugins.core;

/**
 * Created by chuck on 9/25/15.
 */
public class UsherInitializationFailedException extends Exception {
    public UsherInitializationFailedException(Throwable throwable) {
        super(throwable);
    }

    public UsherInitializationFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
