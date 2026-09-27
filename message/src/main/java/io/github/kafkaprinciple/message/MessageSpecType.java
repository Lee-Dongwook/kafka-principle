
package io.github.kafkaprinciple.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum MessageSpecType {
        @JsonProperty("request")
    REQUEST,

        @JsonProperty("response")
    RESPONSE,

        @JsonProperty("header")
    HEADER,

        @JsonProperty("metadata")
    METADATA,

        @JsonProperty("data")
    DATA,

    @JsonProperty("coordinator-key")
    COORDINATOR_KEY,

    @JsonProperty("coordinator-value")
    COORDINATOR_VALUE
}
