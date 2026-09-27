package io.github.kafkaprinciple.message;

import java.util.Objects;

public final class Versions {
    private final short lowest;
    private final short highest;

    public static Versions parse(String input, Versions defaultVersions) {
        if (input == null) {
            return defaultVersions;
        }
        String trimmedInput = input.trim();
        if (trimmedInput.isEmpty()) {
            return defaultVersions;
        }
        if (trimmedInput.equals(NONE_STRING)) {
            return NONE;
        }
        if (trimmedInput.endsWith("+")) {
            return new Versions(Short.parseShort(
            trimmedInput.substring(0, trimmedInput.length() - 1)),
                Short.MAX_VALUE);
        } else {
            int dashIndex = trimmedInput.indexOf("-");
            if (dashIndex < 0) {
                short version = Short.parseShort(trimmedInput);
                return new Versions(version, version);
            }
            return new Versions(
                Short.parseShort(trimmedInput.substring(0, dashIndex)),
                Short.parseShort(trimmedInput.substring(dashIndex + 1)));
        }
    }

    public static final Versions ALL = new Versions((short) 0, Short.MAX_VALUE);

    public static final Versions NONE = new Versions();

    public static final String NONE_STRING = "none";

    private Versions() {
        this.lowest = 0;
        this.highest = -1;
    }

    public Versions(short lowest, short highest) {
        if ((lowest < 0) || (highest < 0)) {
            throw new RuntimeException("Invalid version range " +
                lowest + " to " + highest);
        }
        this.lowest = lowest;
        this.highest = highest;
    }

    public short lowest() {
        return lowest;
    }

    public short highest() {
        return highest;
    }

    public boolean empty() {
        return lowest > highest;
    }

    @Override
    public String toString() {
        if (empty()) {
            return NONE_STRING;
        } else if (lowest == highest) {
            return String.valueOf(lowest);
        } else if (highest == Short.MAX_VALUE) {
            return String.format("%d+", lowest);
        } else {
            return String.format("%d-%d", lowest, highest);
        }
    }

        public Versions intersect(Versions other) {
        short newLowest = lowest > other.lowest ? lowest : other.lowest;
        short newHighest = highest < other.highest ? highest : other.highest;
        if (newLowest > newHighest) {
            return Versions.NONE;
        }
        return new Versions(newLowest, newHighest);
    }

    public Versions subtract(Versions other) {
        if (other.lowest() <= lowest) {
            if (other.highest >= highest) {
                return Versions.NONE;
            } else if (other.highest < lowest) {
                return this;
            } else {
                return new Versions((short) (other.highest() + 1), highest);
            }
        } else if (other.highest >= highest) {
            int newHighest = other.lowest - 1;
            if (newHighest < 0) {
                return this;
            } else if (newHighest < highest) {
                return new Versions(lowest, (short) newHighest);
            } else {
                return this;
            }
        } else {
            return null;
        }
    }

    public boolean contains(short version) {
        return version >= lowest && version <= highest;
    }

    public boolean contains(Versions other) {
        if (other.empty()) {
            return true;
        }
        return !((lowest > other.lowest) || (highest < other.highest));
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowest, highest);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Versions)) {
            return false;
        }
        Versions otherVersions = (Versions) other;
        return lowest == otherVersions.lowest &&
               highest == otherVersions.highest;
    }
}
