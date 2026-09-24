package io.github.kafkaprinciple.common;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/** Immutable 128-bit identifier used by broker metadata. */
@InterfaceAudience.Public
public class Uuid implements Comparable<Uuid> {

    public static final Uuid ONE_UUID = new Uuid(0L, 1L);

    public static final Uuid METADATA_TOPIC_ID = ONE_UUID;

    public static final Uuid ZERO_UUID = new Uuid(0L, 0L);

    public static final Set<Uuid> RESERVED = Set.of(ZERO_UUID, ONE_UUID);

    private final long mostSignificantBits;
    private final long leastSignificantBits;

    private byte[] getBytesFromUuid() {
        ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(this.mostSignificantBits);
        uuidBytes.putLong(this.leastSignificantBits);
        return uuidBytes.array();
    }

    public Uuid(long mostSignificantBits, long leastSignificantBits) {
        this.mostSignificantBits = mostSignificantBits;
        this.leastSignificantBits = leastSignificantBits;
    }

    public static Uuid[] toArray(List<Uuid> list) {
        if (list == null) return null;
        return list.toArray(new Uuid[0]);
    }

    public static List<Uuid> toList(Uuid[] array) {
        if (array == null) return null;
        return new ArrayList<>(Arrays.asList(array));
    }

    public static Uuid randomUuid() {
        while (true) {
            Uuid uuid = unsafeRandomUuid();
            if (!RESERVED.contains(uuid)) {
                return uuid;
            }
        }
    }

    private static Uuid unsafeRandomUuid() {
        java.util.UUID jUuid = java.util.UUID.randomUUID();
        return new Uuid(jUuid.getMostSignificantBits(), jUuid.getLeastSignificantBits());
    }

    public static Uuid fromString(String str) {
        if (str.length() > 24) {
            throw new IllegalArgumentException("Input string with prefix `"
                + str.substring(0, 24) + "` is too long to be decoded as a base64 UUID");
        }

        ByteBuffer uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(str));
        if (uuidBytes.remaining() != 16) {
            throw new IllegalArgumentException("Input string `" + str + "` decoded as "
                + uuidBytes.remaining() + " bytes, which is not equal to the expected 16 bytes "
                + "of a base64-encoded UUID");
        }

        return new Uuid(uuidBytes.getLong(), uuidBytes.getLong());
    }

    public long getMostSignificantBits() {
        return this.mostSignificantBits;
    }

    public long getLeastSignificantBits() {
        return this.leastSignificantBits;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || object.getClass() != getClass()) {
            return false;
        }
        Uuid id = (Uuid) object;
        return this.mostSignificantBits == id.mostSignificantBits &&
            this.leastSignificantBits == id.leastSignificantBits;
        }

    @Override
    public int hashCode() {
        long xor = mostSignificantBits ^ leastSignificantBits;
        return (int) (xor >> 32) ^ (int) xor;
    }

    @Override
    public String toString() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(getBytesFromUuid());
    }

    @Override
    public int compareTo(Uuid other) {
        if (mostSignificantBits > other.mostSignificantBits) {
            return 1;
        } else if (mostSignificantBits < other.mostSignificantBits) {
            return -1;
        } else if (leastSignificantBits > other.leastSignificantBits) {
            return 1;
        } else if (leastSignificantBits < other.leastSignificantBits) {
            return -1;
        } else {
            return 0;
        }
    }
}
