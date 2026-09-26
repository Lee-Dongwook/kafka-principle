package io.github.kafkaprinciple.network;

public enum WakeupRequest implements BaseRequest {
    INSTANCE;

    @Override
    public String toString() {
        return "WakeupRequest";
    }
}
