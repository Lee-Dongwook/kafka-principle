package io.github.kafkaprinciple.common.utils;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils.ByteArrayComparator;

import java.util.Arrays;
import java.util.Objects;

@InterfaceAudience.Public
public class Bytes implements Comparable<Bytes> {
    public static final byte[] EMPTY = new byte[0];

    private static final char[] HEX_CHARS_UPPER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
            'D', 'E', 'F' };
    
    private final byte[] bytes;

    private int hashCode;

    public static Bytes wrap(byte[] bytes) {
        if (bytes == null)
            return null;

        return new Bytes(bytes);
    }
    
    public Bytes(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes cannot be null");
        hashCode = 0;
    }

    public byte[] get() {
        return this.bytes;
    }

    @Override 
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(bytes);
        }

        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;

        if (this.hashCode() != other.hashCode())
            return false;

        if (other instanceof Bytes)
            return Arrays.equals(this.bytes, ((Bytes) other).get());

        return false;
    }

    @Override
    public int compareTo(Bytes that) {
        return ByteUtils.BYTES_LEXICO_COMPARATOR.compare(this.bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Bytes.toString(bytes, 0, bytes.length);
    }


    private static String toString(final byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();

        if (b == null)
            return result.toString();

        if (off >= b.length)
            return result.toString();

        if (off + len > b.length)
            len = b.length - off;

        for (int i = off; i < off + len; ++i) {
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char) ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }

        return result.toString();
    }
    
    private static class LexicographicByteArrayComparator extends ByteUtils.LexicographicByteArrayComparator implements ByteArrayComparator {
    }
}
