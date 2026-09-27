package io.github.kafkaprinciple.common.header.internals;

import io.github.kafkaprinciple.common.header.Header;
import io.github.kafkaprinciple.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class RecordHeader implements Header {
    private ByteBuffer keyBuffer;
    private volatile String key;
    private volatile ByteBuffer valueBuffer;
    private volatile byte[] value;

    public RecordHeader(String key, byte[] value) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.value = value;
    }

    public RecordHeader(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        this.keyBuffer = Objects.requireNonNull(keyBuffer, "Null header keys are not permitted");
        this.valueBuffer = valueBuffer;
    }

    public String key() {
        if (key == null) {
            synchronized (this) {
                if (key == null) {
                    key = Utils.utf8(keyBuffer, keyBuffer.remaining());
                    keyBuffer = null;
                }
            }
        }
        return key;
    }

    public byte[] value() {
        if (value == null && valueBuffer != null) {
            synchronized (this) {
                if (value == null && valueBuffer != null) {
                    value = Utils.toArray(valueBuffer);
                    valueBuffer = null;
                }
            }
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RecordHeader header = (RecordHeader) o;
        return Objects.equals(key(), header.key()) &&
                Arrays.equals(value(), header.value());
    }

    @Override
    public int hashCode() {
        int result = key().hashCode();
        result = 31 * result + Arrays.hashCode(value());
        return result;
    }

    @Override
    public String toString() {
        return "RecordHeader(key = " + key() + ", value = " + Arrays.toString(value()) + ")";
    }
}
