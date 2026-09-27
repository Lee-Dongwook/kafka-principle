package io.github.kafkaprinciple.common.header;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public interface Headers extends Iterable<Header> {
    Headers add(Header header) throws IllegalStateException;

    Headers add(String key, byte[] value) throws IllegalStateException;

    Headers remove(String key) throws IllegalStateException;

    Header lastHeader(String key);

    Iterable<Header> headers(String key);

    Header[] toArray();
}
