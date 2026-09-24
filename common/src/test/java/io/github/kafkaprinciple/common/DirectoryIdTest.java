package io.github.kafkaprinciple.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DirectoryIdTest {

    @Test
    void reservedRecognizesTheReservedRangeOnly() {
        assertTrue(DirectoryId.reserved(new Uuid(0L, 0L)));
        assertTrue(DirectoryId.reserved(new Uuid(0L, 99L)));
        assertFalse(DirectoryId.reserved(new Uuid(0L, 100L)));
        assertFalse(DirectoryId.reserved(new Uuid(1L, 0L)));
    }

    @Test
    void createAssignmentMapPairsEachBrokerWithItsDirectory() {
        Uuid firstDirectory = new Uuid(1L, 1L);
        Uuid secondDirectory = new Uuid(2L, 2L);

        Map<Integer, Uuid> assignments = DirectoryId.createAssignmentMap(
            new int[] {1, 2}, new Uuid[] {firstDirectory, secondDirectory});

        assertEquals(Map.of(1, firstDirectory, 2, secondDirectory), assignments);
    }

    @Test
    void createAssignmentMapRejectsMismatchedLengthsAndDuplicateBrokers() {
        assertThrows(IllegalArgumentException.class, () ->
            DirectoryId.createAssignmentMap(new int[] {1}, new Uuid[] {}));
        assertThrows(IllegalArgumentException.class, () ->
            DirectoryId.createAssignmentMap(new int[] {1, 1}, new Uuid[] {new Uuid(1L, 1L), new Uuid(2L, 2L)}));
    }

    @Test
    void createsArraysWithTheRequestedReservedValue() {
        assertArrayEquals(
            new Uuid[] {DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED},
            DirectoryId.unassignedArray(2));
        assertArrayEquals(
            new Uuid[] {DirectoryId.MIGRATING, DirectoryId.MIGRATING},
            DirectoryId.migratingArray(2));
    }

    @Test
    void isOnlineHandlesReservedLostAndAssignedDirectories() {
        Uuid onlineDirectory = new Uuid(1L, 1L);
        Uuid offlineDirectory = new Uuid(1L, 2L);
        List<Uuid> onlineDirectories = List.of(onlineDirectory);

        assertTrue(DirectoryId.isOnline(DirectoryId.UNASSIGNED, onlineDirectories));
        assertTrue(DirectoryId.isOnline(DirectoryId.MIGRATING, onlineDirectories));
        assertFalse(DirectoryId.isOnline(DirectoryId.LOST, onlineDirectories));
        assertTrue(DirectoryId.isOnline(onlineDirectory, onlineDirectories));
        assertFalse(DirectoryId.isOnline(offlineDirectory, onlineDirectories));
        assertTrue(DirectoryId.isOnline(offlineDirectory, List.of()));
    }

    @Test
    void randomDoesNotReturnAReservedDirectoryId() {
        assertFalse(DirectoryId.reserved(DirectoryId.random()));
    }
}
