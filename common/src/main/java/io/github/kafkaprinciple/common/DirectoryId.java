package io.github.kafkaprinciple.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DirectoryId {
    public static final Uuid MIGRATING = new Uuid(0L, 0L);    
    public static final Uuid UNASSIGNED = new Uuid(0L, 1L);
    public static final Uuid LOST = new Uuid(0L, 2L);

    private DirectoryId() {
    }

    public static Uuid random() {
        while (true) {
            Uuid uuid = Uuid.randomUuid();
            if (!DirectoryId.reserved(uuid)) {
                return uuid;
            }
        }
    }

    public static boolean reserved(Uuid uuid) {
        return uuid.getMostSignificantBits() == 0 &&
            uuid.getLeastSignificantBits() < 100;
    }

    public static Map<Integer, Uuid> createAssignmentMap(int[] replicas, Uuid[] directories) {
        if (replicas.length != directories.length) {
            throw new IllegalArgumentException("The lengths for replicas and directories do not match");
        }

        Map<Integer, Uuid> assignments = new HashMap<>();
        for (int i = 0; i < replicas.length; i++) {
            int brokerId = replicas[i];
            Uuid directory = directories[i];
            if (assignments.put(brokerId, directory) != null) {
                throw new IllegalArgumentException("Duplicate broker ID in assignment");
            }
        }

        return assignments;
    }

    public static Uuid[] unassignedArray(int length) {
        return array(length, UNASSIGNED);
    }

    public static Uuid[] migratingArray(int length) {
        return array(length, MIGRATING);
    }

    private static Uuid[] array(int length, Uuid value) {
        Uuid[] array = new Uuid[length];
        Arrays.fill(array, value);
        return array;
    }

    public static boolean isOnline(Uuid dir, List<Uuid> sortedOnlineDirs) {
        if (UNASSIGNED.equals(dir) || MIGRATING.equals(dir)) {
            return true;
        }

        if (LOST.equals(dir)) {
            return false;
        }

        if (sortedOnlineDirs.isEmpty()) {
            return true;
        }
        
        return Collections.binarySearch(sortedOnlineDirs, dir) >= 0;
    }
}
