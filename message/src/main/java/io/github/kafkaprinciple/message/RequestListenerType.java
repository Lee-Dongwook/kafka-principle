package io.github.kafkaprinciple.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum RequestListenerType {

    @JsonProperty("broker")
    BROKER,

    @JsonProperty("controller")
    CONTROLLER
}
