package io.github.kafkaprinciple.image;

import io.github.kafkaprinciple.image.node.MetadataImageNode;
import io.github.kafkaprinciple.image.writer.ImageWriter;
import io.github.kafkaprinciple.image.writer.ImageWriterOptions;
import io.github.kafkaprinciple.server.common.OffsetAndEpoch;

public record MetadataImage(MetadataProvenance provenance, FeaturesImage features, ClusterImage cluster,
                            TopicsImage topics, ConfigurationsImage configs, ClientQuotasImage clientQuotas,
                            ProducerIdsImage producerIds, AclsImage acls, ScramImage scram,
                            DelegationTokenImage delegationTokens) {
    public static final MetadataImage EMPTY = new MetadataImage(
        MetadataProvenance.EMPTY,
        FeaturesImage.EMPTY,
        ClusterImage.EMPTY,
        TopicsImage.EMPTY,
        ConfigurationsImage.EMPTY,
        ClientQuotasImage.EMPTY,
        ProducerIdsImage.EMPTY,
        AclsImage.EMPTY,
        ScramImage.EMPTY,
        DelegationTokenImage.EMPTY
    );

    public boolean isEmpty() {
        return features.isEmpty() &&
            cluster.isEmpty() &&
            topics.isEmpty() &&
            configs.isEmpty() &&
            clientQuotas.isEmpty() &&
            producerIds.isEmpty() &&
            acls.isEmpty() &&
            scram.isEmpty() &&
            delegationTokens.isEmpty();
    }

    public OffsetAndEpoch highestOffsetAndEpoch() {
        return new OffsetAndEpoch(provenance.lastContainedOffset(), provenance.lastContainedEpoch());
    }

    public long offset() {
        return provenance.lastContainedOffset();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        features.write(writer, options);
        cluster.write(writer, options);
        topics.write(writer, options);
        configs.write(writer);
        clientQuotas.write(writer);
        producerIds.write(writer);
        acls.write(writer);
        scram.write(writer, options);
        delegationTokens.write(writer, options);
        writer.close(true);
    }

    @Override 
    public String toString() {
        return new MetadataImageNode(this).stringify();
    }
}
