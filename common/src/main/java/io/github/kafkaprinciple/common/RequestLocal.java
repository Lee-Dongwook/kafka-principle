package io.github.kafkaprinciple.common;

import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;

import java.util.Objects;

public class RequestLocal implements AutoCloseable {
    private static final RequestLocal NO_CACHING = new RequestLocal(BufferSupplier.NO_CACHING);

    private final BufferSupplier bufferSupplier;

    public RequestLocal(BufferSupplier bufferSupplier) {
        this.bufferSupplier = bufferSupplier;
    }

    public static RequestLocal noCaching() {
        return NO_CACHING;
    }

    public static RequestLocal withThreadConfinedCaching() {
        return new RequestLocal(BufferSupplier.create());
    }

    public BufferSupplier bufferSupplier() {
        return bufferSupplier;
    }

    @Override
    public void close() {
        bufferSupplier.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestLocal that = (RequestLocal) o;
        return Objects.equals(bufferSupplier, that.bufferSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bufferSupplier);
    }

    @Override
    public String toString() {
        return "RequestLocal(bufferSupplier=" + bufferSupplier + ')';
    }
}
