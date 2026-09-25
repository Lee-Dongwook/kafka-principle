package io.github.kafkaprinciple.server.common;

import io.github.kafkaprinciple.common.protocol.ApiMessage;

/** TODO: move to the server module once it exists. */
public record ApiMessageAndVersion(ApiMessage message, short version) { }
