package io.github.kafkaprinciple.server.common;

import java.util.Map;

public interface FeatureVersion {
    short featureLevel();

    String featureName();

    MetadataVersion bootstrapMetadataVersion();

    Map<String, Short> dependencies();
}
