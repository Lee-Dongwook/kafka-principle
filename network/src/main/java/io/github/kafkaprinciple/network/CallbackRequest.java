package io.github.kafkaprinciple.network;

import io.github.kafkaprinciple.common.RequestLocal;

import java.util.function.Consumer;

public record CallbackRequest(Consumer<RequestLocal> fun, Request originalRequest ) implements BaseRequest {
    
    @Override 
    public String toString() {
        return "CallbackRequest(" + fun + ", " + originalRequest + ")";
    }
}
