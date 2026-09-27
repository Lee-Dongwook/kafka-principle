
package io.github.kafkaprinciple.message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class HeaderVersions {
    public static final class Entry {
        private final Versions range;
        private final short headerVersion;

        Entry(Versions range, short headerVersion) {
            this.range = range;
            this.headerVersion = headerVersion;
        }

        public Versions range() {
            return range;
        }

        public short headerVersion() {
            return headerVersion;
        }
    }

    private final List<Entry> entries;

    private HeaderVersions(List<Entry> entries) {
        this.entries = entries;
    }

        public static HeaderVersions parse(String messageName, Map<String, String> raw, Versions validVersions) {
        if (raw == null) {
            return null;
        }
        if (raw.isEmpty()) {
            throw new RuntimeException("Message " + messageName + " specifies an empty headerVersions map.");
        }
        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            entries.add(parseEntry(messageName, entry.getKey(), entry.getValue()));
        }
        entries.sort(Comparator.comparingInt(entry -> entry.range.lowest()));
        validate(messageName, entries, validVersions);
        return new HeaderVersions(entries);
    }

    private static Entry parseEntry(String messageName, String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            throw new RuntimeException("Message " + messageName +
                " specifies a blank version range in headerVersions.");
        }
        Versions range;
        try {
            range = Versions.parse(key, null);
        } catch (RuntimeException e) {
            range = null;
        }
        if (range == null || range.empty()) {
            throw new RuntimeException("Message " + messageName +
                " specifies an invalid version range \"" + key + "\" in headerVersions.");
        }
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException("Message " + messageName +
                " specifies a blank header version for range \"" + key + "\" in headerVersions.");
        }
        short headerVersion;
        try {
            headerVersion = Short.parseShort(value.trim());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Message " + messageName + " specifies an invalid header version \"" +
                value + "\" for range \"" + key + "\" in headerVersions.");
        }
        if (headerVersion < 0) {
            throw new RuntimeException("Message " + messageName + " specifies a negative header version \"" +
                value + "\" for range \"" + key + "\" in headerVersions.");
        }
        return new Entry(range, headerVersion);
    }

    private static void validate(String messageName, List<Entry> entries, Versions validVersions) {
        if (entries.get(0).range.lowest() != 0) {
            throw new RuntimeException("Message " + messageName + " has headerVersions starting at version " +
                entries.get(0).range.lowest() + ", but the first range must start at version 0 so that the map " +
                "covers every version the schema describes, including versions that are no longer valid.");
        }
        for (int i = 1; i < entries.size(); i++) {
            Entry previousEntry = entries.get(i - 1);
            Entry currentEntry = entries.get(i);
            Versions previous = previousEntry.range;
            Versions current = currentEntry.range;
            if (previous.highest() == Short.MAX_VALUE) {
                throw new RuntimeException("Message " + messageName + " has headerVersions range " + previous +
                    ", which is open-ended (ends in '+'), but is followed by " + current + "; only the last " +
                    "range may be open-ended — every earlier range must be bounded, e.g. {\"0-1\": \"1\", " +
                    "\"2+\": \"2\"}.");
            }
            int expected = previous.highest() + 1;
            if (current.lowest() < expected) {
                throw new RuntimeException("Message " + messageName + " has overlapping headerVersions ranges " +
                    previous + " and " + current + ": both cover version " + current.lowest() + ". Ranges must " +
                    "be contiguous and non-overlapping, e.g. {\"0-1\": \"1\", \"2+\": \"2\"}.");
            }
            if (current.lowest() > expected) {
                throw new RuntimeException("Message " + messageName + " has non-contiguous headerVersions: the " +
                    "range after " + previous + " must start at version " + expected +
                    ", but it starts at version " + current.lowest() + ".");
            }
            if (currentEntry.headerVersion < previousEntry.headerVersion) {
                throw new RuntimeException("Message " + messageName + " maps the higher message version range " + current +
                    " to header version " + currentEntry.headerVersion + ", which is lower than header version " +
                    previousEntry.headerVersion + " used by the earlier range " + previous +
                    "; header versions must not decrease as message versions increase.");
            }
        }
        Entry last = entries.get(entries.size() - 1);
        if (last.range.highest() != Short.MAX_VALUE) {
            throw new RuntimeException("Message " + messageName + " has headerVersions whose last range " +
                last.range + " is not open-ended; the last range must end with a plus sign.");
        }
        for (Entry entry : entries) {
            if (entry.range.lowest() > validVersions.highest()) {
                throw new RuntimeException("Message " + messageName + " has a headerVersions range " + entry.range +
                    " that starts above the highest valid version " + validVersions.highest() + ".");
            }
        }
    }

    public List<Entry> entries() {
        return entries;
    }

        public Map<String, String> toMap() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (Entry entry : entries) {
            map.put(entry.range.toString(), Short.toString(entry.headerVersion));
        }
        return map;
    }
}
