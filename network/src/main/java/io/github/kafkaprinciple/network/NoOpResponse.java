package io.github.kafkaprinciple.network;

public record NoOpResponse(Request request) implements Response {
    
    @Override 
    public String toString() {
        return "Response(type=NoOp, request=" + request + ")";
    }
}
