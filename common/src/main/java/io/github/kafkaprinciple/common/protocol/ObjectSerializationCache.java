package io.github.kafkaprinciple.common.protocol;

import java.util.IdentityHashMap;

public final class ObjectSerializationCache {
    private final IdentityHashMap<Object, Object> map;

    public ObjectSerializationCache() {
        this.map = new IdentityHashMap<>();
    }

    public void setArraySizeInBytes(Object o, Integer size) {
        map.put(o, size);
    }

    public Integer getArraySizeInBytes(Object o) {
        return (Integer) map.get(o);
    }

    public void cacheSerializedValue(Object o, byte[] val) {
        map.put(o, val);
    }

    public byte[] getSerializedValue(Object o) {
        Object value = map.get(o);
        return (byte[]) value;
    }
}
