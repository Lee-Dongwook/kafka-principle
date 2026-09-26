package io.github.kafkaprinciple.network;

public sealed interface BaseRequest permits CallbackRequest, Request, ShutdownRequest, WakeupRequest {
    
}
