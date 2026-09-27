package io.github.kafkaprinciple.server.common;

import io.github.kafkaprinciple.common.feature.SupportedVersionRange;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.github.kafkaprinciple.server.common.UnitTestFeatureVersion.FV0.UT_FV0_0;

public enum Feature {
    KRAFT_VERSION(KRaftVersion.FEATURE_NAME, KRaftVersion.values(), KRaftVersion.LATEST_PRODUCTION),
    TRANSACTION_VERSION(TransactionVersion.FEATURE_NAME, TransactionVersion.values(), TransactionVersion.LATEST_PRODUCTION),
    GROUP_VERSION(GroupVersion.FEATURE_NAME, GroupVersion.values(), GroupVersion.LATEST_PRODUCTION),
    ELIGIBLE_LEADER_REPLICAS_VERSION(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.values(), EligibleLeaderReplicasVersion.LATEST_PRODUCTION),
    SHARE_VERSION(ShareVersion.FEATURE_NAME, ShareVersion.values(), ShareVersion.LATEST_PRODUCTION),
    STREAMS_VERSION(StreamsVersion.FEATURE_NAME, StreamsVersion.values(), StreamsVersion.LATEST_PRODUCTION),

    TEST_VERSION(TestFeatureVersion.FEATURE_NAME, TestFeatureVersion.values(), TestFeatureVersion.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_0(UnitTestFeatureVersion.FV0.FEATURE_NAME, new FeatureVersion[]{UT_FV0_0}, UnitTestFeatureVersion.FV0.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_1(UnitTestFeatureVersion.FV1.FEATURE_NAME, UnitTestFeatureVersion.FV1.values(), UnitTestFeatureVersion.FV1.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_2(UnitTestFeatureVersion.FV2.FEATURE_NAME, UnitTestFeatureVersion.FV2.values(), UnitTestFeatureVersion.FV2.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_3(UnitTestFeatureVersion.FV3.FEATURE_NAME, UnitTestFeatureVersion.FV3.values(), UnitTestFeatureVersion.FV3.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_4(UnitTestFeatureVersion.FV4.FEATURE_NAME, UnitTestFeatureVersion.FV4.values(), UnitTestFeatureVersion.FV4.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_5(UnitTestFeatureVersion.FV5.FEATURE_NAME, UnitTestFeatureVersion.FV5.values(), UnitTestFeatureVersion.FV5.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_6(UnitTestFeatureVersion.FV6.FEATURE_NAME, UnitTestFeatureVersion.FV6.values(), UnitTestFeatureVersion.FV6.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_7(UnitTestFeatureVersion.FV7.FEATURE_NAME, UnitTestFeatureVersion.FV7.values(), UnitTestFeatureVersion.FV7.LATEST_PRODUCTION);

    public static final Feature[] FEATURES;

    public static final List<Feature> TEST_AND_PRODUCTION_FEATURES;

    public static final List<Feature> PRODUCTION_FEATURES;

    public static final List<String> PRODUCTION_FEATURE_NAMES;
    private final String name;
    private final FeatureVersion[] featureVersions;

    public final FeatureVersion latestProduction;

    Feature(String name,
            FeatureVersion[] featureVersions,
            FeatureVersion latestProduction
    ) {
        this.name = name;
        this.featureVersions = featureVersions;
        this.latestProduction = latestProduction;
    }

    static {
        Feature[] enumValues = Feature.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        TEST_AND_PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature -> 
            !feature.name.startsWith("unit." + TestFeatureVersion.FEATURE_NAME)
        ).toList();

        PRODUCTION_FEATURE_NAMES = PRODUCTION_FEATURES.stream().map(feature ->
                feature.name).toList();
        
        validateDefaultValueAndLatestProductionValue(TEST_VERSION);
        for (Feature feature : PRODUCTION_FEATURES) {
            validateDefaultValueAndLatestProductionValue(feature);
        }
    }

    public String featureName() {
        return name;
    }

    public FeatureVersion[] featureVersions() {
        return featureVersions;
    }

    public short latestProduction() {
        return latestProduction.featureLevel();
    }

    public short minimumProduction() {
        return featureVersions[0].featureLevel();
    }

    public short latestTesting() {
        return featureVersions[featureVersions.length - 1].featureLevel();
    }

    public SupportedVersionRange supportedVersionRange(boolean includeUnstable) {
        return new SupportedVersionRange(
            minimumProduction(),
            includeUnstable ? latestTesting() : latestProduction()
        );
    }

    public FeatureVersion fromFeatureLevel(short level,
                                           boolean allowUnstableFeatureVersions) {
        return Arrays.stream(featureVersions).filter(featureVersion ->
            featureVersion.featureLevel() == level && (allowUnstableFeatureVersions || level <= latestProduction())).findFirst().orElseThrow(
                () -> new IllegalArgumentException("No feature:" + featureName() + " with feature level " + level));
    }

    pubic static void validateVersion(FeatureVersion feature, Map<String, Short> features) {
        Short metadataVersion = features.get(MetadataVersion.FEATURE_NAME);

        if(feature.freatureLevel() >= 1 && (metadataVersion == null || metadataVersion < MetadataVersion.MINIMUM_VERSION.featureLevel())) {
             throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                    " because it depends on metadata.version=" + MetadataVersion.MINIMUM_VERSION.featureLevel() + " (" + MetadataVersion.MINIMUM_VERSION + ")");
        }

        for (Map.Entry<String, Short> dependency: feature.dependencies().entrySet()) {
            Short featureLevel = features.get(dependency.getKey());

            if(featureLevel == null || featureLevel < dependency.getValue()) {
                throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                        " because it depends on " + dependency.getKey() + " level " + dependency.getValue());
            }
        }
    }

    public FeatureVersion defaultVersion(MetadataVersion metadataVersion) {
        FeatureVersion version = featureVersions[0];
        for (Iterator<FeatureVersion> it = Arrays.stream(featureVersions).iterator(); it.hasNext(); ) {
            FeatureVersion feature = it.next();
            if (feature.bootstrapMetadataVersion().isLessThan(metadataVersion) || feature.bootstrapMetadataVersion().equals(metadataVersion))
                version = feature;
            else
                return version;
        }
        return version;
    }

    public short defaultLevel(MetadataVersion metadataVersion) {
        return defaultVersion(metadataVersion).featureLevel();
    }

    public static Feature featureFromName(String featureName) {
        for (Feature feature : FEATURES) {
            if (feature.name.equals(featureName))
                return feature;
        }
        throw new IllegalArgumentException("Feature " + featureName + " not found.");
    }

    public boolean isProductionReady(short featureVersion) {
        return featureVersion <= latestProduction();
    }

    public boolean hasFeatureVersion(FeatureVersion featureVersion) {
        for (FeatureVersion v : featureVersions()) {
            if (v == featureVersion) {
                return true;
            }
        }
        return false;
    }

    public static void validateDefaultValueAndLatestProductionValue(
        Feature feature
    ) throws IllegalArgumentException {
        FeatureVersion defaultVersion = feature.defaultVersion(MetadataVersion.LATEST_PRODUCTION);
        FeatureVersion latestProduction = feature.latestProduction;

        if (!feature.hasFeatureVersion(latestProduction)) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production version %s " +
                    "which is not one of its feature versions.", feature.name(), latestProduction));
        }

        if (latestProduction.featureLevel() < defaultVersion.featureLevel()) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production value %s " +
                    "smaller than its default version %s with latest production MV.",
                feature.name(), latestProduction, defaultVersion));
        }

        for (Map.Entry<String, Short> dependency: latestProduction.dependencies().entrySet()) {
            String dependencyFeatureName = dependency.getKey();

            if(!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                Feature dependencyFeature = featureFromName(dependencyFeatureName);

                if(!dependencyFeature.isProductionReady(dependency.getValue())) {
                    throw new IllegalArgumentException(String.format("Feature %s has latest production FeatureVersion %s " +
                            "with dependency %s that is not production ready. (%s latest production: %s)",
                            feature.name(), latestProduction, dependencyFeature.fromFeatureLevel(dependency.getValue(), true),
                            dependencyFeature, dependencyFeature.latestProduction));
                }
            } else {
                if (dependency.getValue() > MetadataVersion.LATEST_PRODUCTION.featureLevel()) {
                    throw new IllegalArgumentException(String.format("Feature %s has latest production FeatureVersion %s " +
                            "with MV dependency %s that is not production ready. (MV latest production: %s)",
                        feature.name(), latestProduction, MetadataVersion.fromFeatureLevel(dependency.getValue()),
                        MetadataVersion.LATEST_PRODUCTION));
                }
            }
        }

        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            defaultVersion = feature.defaultVersion(metadataVersion);

            for(Map.Entry<String, Short> dependency : defaultVersion.dependencies().entrySet()) {
                String dependencyFeatureName = dependency.getKey();
                if (!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                    Feature dependencyFeature = featureFromName(dependencyFeatureName);
                    dependency.getValue() > dependencyFeature.defaultLevel(metadataVersion)) {
                        throw new IllegalArgumentException(String.format("Feature %s has default FeatureVersion %s " +
                                "when MV=%s with dependency %s that is behind its default version %s.",
                            feature.name(), defaultVersion, metadataVersion,
                            dependencyFeature.fromFeatureLevel(dependency.getValue(), true),
                            dependencyFeature.defaultVersion(metadataVersion)));
                    }
                } else {
                    if(dependency.getValue() > defaultVersion.bootstrapMetadataVersion().featureLevel()) {
                        throw new IllegalArgumentException(String.format("Feature %s has default FeatureVersion %s " +
                                "when MV=%s with MV dependency %s that is behind its bootstrap MV %s.",
                                feature.name(), defaultVersion, metadataVersion,
                                MetadataVersion.fromFeatureLevel(dependency.getValue()),
                                defaultVersion.bootstrapMetadataVersion()
                        ));
                    }
                }
            }
        }
    }
}
