package io.github.kafkaprinciple.network;

public enum ShutdownRequest implements BaseRequest {
    INSTANCE;

    @Override 
    public String toString() {
        return "ShutdownRequest";
    }
}
