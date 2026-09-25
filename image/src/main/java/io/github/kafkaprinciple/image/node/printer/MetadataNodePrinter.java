package io.github.kafkaprinciple.image.node.printer;

public interface MetadataNodePrinter extends AutoCloseable {
    MetadataNodeRedactionCriteria redactionCriteria();

    void enterNode(String name);

    void leaveNode();

    void output(String text);

    default void close() {
        
    }
}
