package io.github.kafkaprinciple.common.header;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public interface Header {
    String key();
    byte[] value();
}
