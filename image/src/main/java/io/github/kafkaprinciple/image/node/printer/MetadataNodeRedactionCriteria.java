package io.github.kafkaprinciple.image.node.printer;

public interface MetadataNodeRedactionCriteria {
    /** TODO: define redaction rules for sensitive metadata. */
    enum Strict implements MetadataNodeRedactionCriteria {
        INSTANCE
    }
}
