package io.github.kafkaprinciple.common.protocol.types;

import java.util.Arrays;

public class RawTaggedField {
    private final int tag;
    private final byte[] data;
    
    public RawTaggedField(int tag, byte[] data) {
        this.tag = tag;
        this.data = data;
    }

    public int tag() {
        return tag;
    }

    public byte[] data() {
        return data;
    }

    public int size() {
        return data.length;
    }

    @Override 
    public boolean equals(Object o) {
        if ((o == null) || (!o.getClass().equals(getClass()))) {
            return false;
        }

        RawTaggedField other = (RawTaggedField) o;
        return tag == other.tag && Arrays.equals(data, other.data);
    }

    @Override 
    public int hashCode() {
        return tag ^ Arrays.hashCode(data);
    }
}
