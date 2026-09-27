package io.github.kafkaprinciple.common.feature;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public class SupportedVersionRange {
    private final short minVersion;

    private final short maxVersion;

    public SupportedVersionRange(final short minVersion, final short maxVersion) {
        if (minVersion < 0 || maxVersion < 0 || maxVersion < minVersion) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected 0 <= minVersion <= maxVersion but received minVersion:%d, maxVersion:%d.",
                            minVersion,
                            maxVersion));
        }
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public short minVersion() {
        return minVersion;
    }

    public short maxVersion() {
        return maxVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SupportedVersionRange that = (SupportedVersionRange) other;
        return this.minVersion == that.minVersion && this.maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersion, maxVersion);
    }

    @Override
    public String toString() {
        return String.format("SupportedVersionRange[min_version:%d, max_version:%d]", minVersion, maxVersion);
    }
}
