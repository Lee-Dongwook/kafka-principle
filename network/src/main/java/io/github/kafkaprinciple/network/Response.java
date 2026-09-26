package io.github.kafkaprinciple.network;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;


public sealed interface Response permits SendResponse, NoOpResponse {
    Request request();

    default Optional<JsonNode> responseLog() {
        return Optional.empty();
    }
}
