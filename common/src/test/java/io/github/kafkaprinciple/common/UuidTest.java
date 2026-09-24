package io.github.kafkaprinciple.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UuidTest {

    @Test
    void sameBitsAreEqualAndProduceTheSameHashCode() {
        Uuid first = new Uuid(10L, 20L);
        Uuid second = new Uuid(10L, 20L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, new Uuid(10L, 21L));
    }

    @Test
    void toStringUsesTheStandardUuidFormat() {
        Uuid uuid = new Uuid(0x123e4567e89b12d3L, 0xa456426614174000L);

        assertEquals("123e4567-e89b-12d3-a456-426614174000", uuid.toString());
    }

    @Test
    void compareToUsesUnsignedBitOrdering() {
        Uuid unsignedLarge = new Uuid(-1L, 0L);
        Uuid unsignedSmall = new Uuid(0L, -1L);
        Uuid sameMostSignificantBitsWithLargerLeastSignificantBits = new Uuid(0L, 1L);

        assertTrue(unsignedLarge.compareTo(unsignedSmall) > 0);
        assertTrue(sameMostSignificantBitsWithLargerLeastSignificantBits.compareTo(unsignedSmall) < 0);
    }
}
