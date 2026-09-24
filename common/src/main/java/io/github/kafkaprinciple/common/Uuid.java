package io.github.kafkaprinciple.common;

import java.util.Objects;
import java.util.UUID;

/** Immutable 128-bit identifier used by broker metadata. */
public final class Uuid implements Comparable<Uuid> {
    private final long mostSignificantBits;
    private final long leastSignificantBits;

    public Uuid(long mostSignificantBits, long leastSignificantBits) {
        this.mostSignificantBits = mostSignificantBits;
        this.leastSignificantBits = leastSignificantBits;
    }

    public static Uuid randomUuid() {
        UUID uuid = UUID.randomUUID();
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public long getMostSignificantBits() {
        return mostSignificantBits;
    }

    public long getLeastSignificantBits() {
        return leastSignificantBits;
    }

    @Override
    public int compareTo(Uuid other) {
        int mostSignificantComparison = Long.compareUnsigned(mostSignificantBits, other.mostSignificantBits);
        return mostSignificantComparison != 0
            ? mostSignificantComparison
            : Long.compareUnsigned(leastSignificantBits, other.leastSignificantBits);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Uuid other)) {
            return false;
        }
        return mostSignificantBits == other.mostSignificantBits
            && leastSignificantBits == other.leastSignificantBits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mostSignificantBits, leastSignificantBits);
    }

    @Override
    public String toString() {
        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }
}
