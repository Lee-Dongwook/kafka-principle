package io.github.kafkaprinciple.server.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public enum MetadataVersion {
    IBP_3_3_IV3(7, "3.3", "IV3", true),
    IBP_3_4_IV0(8, "3.4", "IV0", true),
    IBP_3_5_IV0(9, "3.5", "IV0", false),
    IBP_3_5_IV1(10, "3.5", "IV1", false),
    IBP_3_5_IV2(11, "3.5", "IV2", true),
    IBP_3_6_IV0(12, "3.6", "IV0", false),
    IBP_3_6_IV1(13, "3.6", "IV1", true),
    IBP_3_6_IV2(14, "3.6", "IV2", true),
    IBP_3_7_IV0(15, "3.7", "IV0", true),
    IBP_3_7_IV1(16, "3.7", "IV1", false),
    IBP_3_7_IV2(17, "3.7", "IV2", true),
    IBP_3_7_IV3(18, "3.7", "IV3", false),
    IBP_3_7_IV4(19, "3.7", "IV4", false),
    IBP_3_8_IV0(20, "3.8", "IV0", false),
    IBP_3_9_IV0(21, "3.9", "IV0", false),
    IBP_4_0_IV0(22, "4.0", "IV0", false),
    IBP_4_0_IV1(23, "4.0", "IV1", true),
    IBP_4_0_IV2(24, "4.0", "IV2", false),
    IBP_4_0_IV3(25, "4.0", "IV3", false),
    IBP_4_1_IV0(26, "4.1", "IV0", false),
    IBP_4_1_IV1(27, "4.1", "IV1", false),
    IBP_4_2_IV0(28, "4.2", "IV0", false),
    IBP_4_2_IV1(29, "4.2", "IV1", false),
    IBP_4_3_IV0(30, "4.3", "IV0", true),
    IBP_4_4_IV0(31, "4.4", "IV0", false),
    IBP_4_4_IV1(32, "4.4", "IV1", true),
    IBP_4_4_IV2(33, "4.4", "IV2", true),
    IBP_4_5_IV0(34, "4.5", "IV0", false);

    public static final String FEATURE_NAME = "metadata.version";

    public static final MetadataVersion MINIMUM_VERSION = IBP_3_3_IV3;

    public static final MetadataVersion LATEST_PRODUCTION = IBP_4_4_IV2;

    public static final MetadataVersion[] VERSIONS;

    private final short featureLevel;
    private final String release;
    private final String ibpVersion;
    private final boolean didMetadataChange;

    MetadataVersion(int featureLevel, String release, String subVersion, boolean didMetadataChange) {
        this.featureLevel = (short) featureLevel;
        this.release = release;
        if (subVersion.isEmpty()) {
            this.ibpVersion = release;
        } else {
            this.ibpVersion = String.format("%s-%s", release, subVersion);
        }
        this.didMetadataChange = didMetadataChange;
    }

    public String featureName() {
        return FEATURE_NAME;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public boolean isScramSupported() {
        return this.isAtLeast(IBP_3_5_IV2);
    }

    public boolean isLeaderEpochBumpRequiredOnIsrShrink() {
        return !this.isAtLeast(IBP_3_6_IV0);
    }

    public boolean isMetadataTransactionSupported() {
        return this.isAtLeast(IBP_3_6_IV1);
    }

    public boolean isDelegationTokenSupported() {
        return this.isAtLeast(IBP_3_6_IV2);
    }

    public boolean isDirectoryAssignmentSupported() {
        return this.isAtLeast(IBP_3_7_IV2);
    }

    public boolean isElrSupported() {
        return this.isAtLeast(IBP_4_0_IV1);
    }

    public boolean isCidrAclSupported() {
        return this.isAtLeast(IBP_4_4_IV1);
    }

    public boolean isMigrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_4_IV0);
    }

    public boolean isCordonedLogDirsSupported() {
        return this.isAtLeast(MetadataVersion.IBP_4_3_IV0);
    }

    public short registerBrokerRecordVersion() {
        if (isCordonedLogDirsSupported()) {
            // new cordonedLogDirs field
            return (short) 4;
        } else if (isDirectoryAssignmentSupported()) {
            // new logDirs field
            return (short) 3;
        } else if (isMigrationSupported()) {
            // new isMigrationZkBroker field
            return (short) 2;
        } else {
            return (short) 1;
        }
    }

    public short registerControllerRecordVersion() {
        if (isAtLeast(MetadataVersion.IBP_3_7_IV0)) {
            return (short) 0;
        } else {
            throw new RuntimeException("Controller registration is not supported in " +
                    "MetadataVersion " + this);
        }
    }

    public boolean isControllerRegistrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_3_7_IV0);
    }

    public boolean isControllerUnregistrationSupported() {
        return this.isAtLeast(MetadataVersion.IBP_4_4_IV2);
    }

    public short partitionChangeRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short partitionRecordVersion() {
        if (isElrSupported()) {
            return (short) 2;
        } else if (isDirectoryAssignmentSupported()) {
            return (short) 1;
        } else {
            return (short) 0;
        }
    }

    public short fetchRequestVersion() {
        if (isAtLeast(IBP_4_1_IV1)) {
            return 18;
        } else if (isAtLeast(IBP_3_9_IV0)) {
            return 17;
        } else if (isAtLeast(IBP_3_7_IV4)) {
            return 16;
        } else if (isAtLeast(IBP_3_5_IV1)) {
            return 15;
        } else if (isAtLeast(IBP_3_5_IV0)) {
            return 14;
        } else {
            return 13;
        }
    }

    public short listOffsetRequestVersion() {
        if (this.isAtLeast(IBP_4_2_IV1)) {
            return 11;
        } else if (this.isAtLeast(IBP_4_0_IV3)) {
            return 10;
        } else if (this.isAtLeast(IBP_3_9_IV0)) {
            return 9;
        } else if (this.isAtLeast(IBP_3_5_IV0)) {
            return 8;
        } else {
            return 7;
        }
    }

    private static final Map<String, MetadataVersion> IBP_VERSIONS;

    static {
        MetadataVersion[] enumValues = MetadataVersion.values();
        VERSIONS = Arrays.copyOf(enumValues, enumValues.length);

        IBP_VERSIONS = new HashMap<>();
        Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
        for (MetadataVersion metadataVersion : VERSIONS) {
            if (metadataVersion.isProduction()) {
                maxInterVersion.put(metadataVersion.release, metadataVersion);
            }
            IBP_VERSIONS.put(metadataVersion.ibpVersion, metadataVersion);
        }
        IBP_VERSIONS.putAll(maxInterVersion);
    }

    public boolean isProduction() {
        return this.compareTo(MetadataVersion.LATEST_PRODUCTION) <= 0;
    }

    public String shortVersion() {
        return release;
    }

    public String version() {
        return ibpVersion;
    }

    public boolean didMetadataChange() {
        return didMetadataChange;
    }

    Optional<MetadataVersion> previous() {
        int idx = this.ordinal();
        if (idx > 0) {
            return Optional.of(VERSIONS[idx - 1]);
        } else {
            return Optional.empty();
        }
    }

    public static MetadataVersion fromVersionString(String versionString, boolean unstableFeatureVersionsEnabled) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }

        MetadataVersion metadataVersion = IBP_VERSIONS.get(key);
        if (metadataVersion == null || (!unstableFeatureVersionsEnabled && !metadataVersion.isProduction())) {
            String errorMsg = "Unknown metadata.version '" + versionString + "'. Supported metadata.version are: "
                + metadataVersionsToString(MetadataVersion.MINIMUM_VERSION,
                unstableFeatureVersionsEnabled ? MetadataVersion.latestTesting() : MetadataVersion.latestProduction());
            throw new IllegalArgumentException(errorMsg);
        }
        return metadataVersion;
    }

    public static String metadataVersionsToString(MetadataVersion first, MetadataVersion last) {
        List<MetadataVersion> versions = List.of(MetadataVersion.VERSIONS).subList(first.ordinal(), last.ordinal() + 1);
        return versions.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));
    }

    public static MetadataVersion fromFeatureLevel(short version) {
        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            if (metadataVersion.featureLevel() == version) {
                return metadataVersion;
            }
        }
        throw new IllegalArgumentException("No MetadataVersion with feature level " + version + ". Valid feature levels are from "
            + MINIMUM_VERSION.featureLevel + " to " + latestTesting().featureLevel + ".");
    }

    public static MetadataVersion latestTesting() {
        return VERSIONS[VERSIONS.length - 1];
    }

    public static MetadataVersion latestProduction() {
        return LATEST_PRODUCTION;
    }

    public static boolean checkIfMetadataChanged(MetadataVersion sourceVersion, MetadataVersion targetVersion) {
        if (sourceVersion == targetVersion) {
            return false;
        }

        final MetadataVersion highVersion, lowVersion;
        if (sourceVersion.compareTo(targetVersion) < 0) {
            highVersion = targetVersion;
            lowVersion = sourceVersion;
        } else {
            highVersion = sourceVersion;
            lowVersion = targetVersion;
        }
        return checkIfMetadataChangedOrdered(highVersion, lowVersion);
    }

    private static boolean checkIfMetadataChangedOrdered(MetadataVersion highVersion, MetadataVersion lowVersion) {
        MetadataVersion version = highVersion;
        while (!version.didMetadataChange() && version != lowVersion) {
            Optional<MetadataVersion> prev = version.previous();
            if (prev.isPresent()) {
                version = prev.get();
            } else {
                break;
            }
        }
        return version != lowVersion;
    }

    public boolean isAtLeast(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(MetadataVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }

    @Override
    public String toString() {
        return ibpVersion;
    }
}
