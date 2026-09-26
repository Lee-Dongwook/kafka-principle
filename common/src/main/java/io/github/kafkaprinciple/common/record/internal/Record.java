package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.header.Header;
import io.github.kafkaprinciple.common.record.TimestampType;

import java.nio.ByteBuffer;

public interface Record {
    Header[] EMPTY_HEADERS = new Header[0];

    long offset();

    int sequence();

    int sizeInBytes();

    long timestamp();

    void ensureValid();

    int keySize();

    boolean hasKey();

    ByteBuffer key();

    int valueSize();

    boolean hasValue();

    ByteBuffer value();

    boolean hasMagic(byte magic);

    boolean isCompressed();

    boolean hasTimestampType(TimestampType timestampType);

    Header[] headers();
}
