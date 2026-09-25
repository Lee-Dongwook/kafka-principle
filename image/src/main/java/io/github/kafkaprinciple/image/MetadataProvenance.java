package io.github.kafkaprinciple.image;

/** TODO: retain provenance for a metadata image. */
public final class MetadataProvenance {
    public static final MetadataProvenance EMPTY = new MetadataProvenance();
    public long lastContainedOffset() { return -1L; }
    public int lastContainedEpoch() { return -1; }
}
