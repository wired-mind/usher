package io.cozmic.usher.plugins.journaling;

import kafka.common.ErrorMapping;

/**
 * ConsumerOffsetsException
 * Created by Craig Earley on 9/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class ConsumerOffsetsException extends Exception {

    private static final long serialVersionUID = 1L;

    private short errorCode = ErrorMapping.UnknownCode();

    public ConsumerOffsetsException(Throwable t) {
        super(t);
    }

    public ConsumerOffsetsException(String message) {
        super(message);
    }

    public ConsumerOffsetsException(String message, short errorCode) {
        super(formattedMessage(message, errorCode));
        this.errorCode = errorCode;
    }

    private static String formattedMessage(String message, short errorCode) {
        if (errorCode != ErrorMapping.UnknownCode()) {
            if (message == null) {
                message = "";
            }
            return String.format("%s : Kafka error mapping: code %s -> %s", message, errorCode,
                    ErrorMapping.exceptionFor(errorCode).getClass().getSimpleName());
        }
        return message;
    }

    public short getErrorCode() {
        return errorCode;
    }
}
