package io.github.kafkaprinciple.image.node;

import io.github.kafkaprinciple.image.MetadataImage;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public record MetadataImageNode(MetadataImage image) implements MetadataNode {
    public static final String NAME = "image";

    private static final Map<String, Function<MetadataImage, MetadataNode>> CHILDREN = Map.of(
        ProvenanceNode.NAME, image -> new ProvenanceNode(image.provenance()),
        FeaturesImageNode.NAME, image -> new FeaturesImageNode(image.features()),
        ClusterImageNode.NAME, image -> new ClusterImageNode(image.cluster()),
        TopicsImageNode.NAME, image -> new TopicsImageNode(image.topics()),
        ConfigurationsImageNode.NAME, image -> new ConfigurationsImageNode(image.configs()),
        ClientQuotasImageNode.NAME, image -> new ClientQuotasImageNode(image.clientQuotas()),
        ProducerIdsImageNode.NAME, image -> new ProducerIdsImageNode(image.producerIds()),
        AclsImageNode.NAME, image -> new AclsImageNode(image.acls()),
        ScramImageNode.NAME, image -> new ScramImageNode(image.scram()),
        DelegationTokenImageNode.NAME, image -> new DelegationTokenImageNode(image.delegationTokens())
    );

    @Override
    public Collection<String> childNames() {
        return CHILDREN.keySet();
    }

    @Override 
    public MetadataNode child(String name) {
        return CHILDREN.getOrDefault(name, __ -> null).apply(image);
    }
}
