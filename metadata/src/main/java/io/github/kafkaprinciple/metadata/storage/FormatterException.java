package io.github.kafkaprinciple.metadata.storage;

public class FormatterException extends RuntimeException {
    public FormatterException(String what) {
        super(what);
    }

    public FormatterException(String what, Exception cause) {
        super(what, cause);
    }
}
