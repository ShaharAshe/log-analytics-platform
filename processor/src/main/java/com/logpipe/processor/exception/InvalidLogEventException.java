package com.logpipe.processor.exception;

public class InvalidLogEventException extends RuntimeException {
    public InvalidLogEventException(String message) {
        super(message);
    }
}
