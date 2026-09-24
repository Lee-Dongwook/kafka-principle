package io.github.kafkaprinciple.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
    void serializesAsUrlSafeBase64WithoutPadding() {
        Uuid uuid = new Uuid(0x123e4567e89b12d3L, 0xa456426614174000L);

        assertEquals("Ej5FZ-ibEtOkVkJmFBdAAA", uuid.toString());
        assertEquals(uuid, Uuid.fromString(uuid.toString()));
    }

    @Test
    void fromStringRejectsInvalidEncodedValues() {
        assertThrows(IllegalArgumentException.class, () -> Uuid.fromString("a".repeat(25)));
        assertThrows(IllegalArgumentException.class, () -> Uuid.fromString("AA"));
        assertThrows(IllegalArgumentException.class, () -> Uuid.fromString("not-a-base64-uuid?"));
    }

    @Test
    void exposesReservedUuidConstants() {
        assertEquals(new Uuid(0L, 0L), Uuid.ZERO_UUID);
        assertEquals(new Uuid(0L, 1L), Uuid.ONE_UUID);
        assertEquals(Uuid.ONE_UUID, Uuid.METADATA_TOPIC_ID);
        assertEquals(java.util.Set.of(Uuid.ZERO_UUID, Uuid.ONE_UUID), Uuid.RESERVED);
    }

    @Test
    void convertsBetweenUuidArraysAndLists() {
        Uuid first = new Uuid(1L, 1L);
        Uuid second = new Uuid(2L, 2L);
        Uuid[] uuids = {first, second};

        assertArrayEquals(uuids, Uuid.toArray(List.of(first, second)));
        assertEquals(List.of(first, second), Uuid.toList(uuids));
        assertNull(Uuid.toArray(null));
        assertNull(Uuid.toList(null));
    }

    @Test
    void randomUuidDoesNotReturnAReservedUuid() {
        for (int i = 0; i < 20; i++) {
            assertFalse(Uuid.RESERVED.contains(Uuid.randomUuid()));
        }
    }

    @Test
    void compareToUsesSignedBitOrdering() {
        Uuid negativeMostSignificantBits = new Uuid(-1L, 0L);
        Uuid zeroMostSignificantBits = new Uuid(0L, -1L);
        Uuid largerLeastSignificantBits = new Uuid(0L, 1L);

        assertTrue(negativeMostSignificantBits.compareTo(zeroMostSignificantBits) < 0);
        assertTrue(largerLeastSignificantBits.compareTo(zeroMostSignificantBits) > 0);
    }
}
