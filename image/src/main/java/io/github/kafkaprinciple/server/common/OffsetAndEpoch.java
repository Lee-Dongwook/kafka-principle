package io.github.kafkaprinciple.server.common;

/** TODO: move to the server module once it exists. */
public record OffsetAndEpoch(long offset, int epoch) { }
