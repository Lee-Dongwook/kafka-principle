package io.github.kafkaprinciple.image.node;

import io.github.kafkaprinciple.image.node.printer.MetadataNodePrinter;
import io.github.kafkaprinciple.image.node.printer.NodeStringifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface MetadataNode {
    default boolean isDirectory() {
        return true;
    }

    default Collection<String> childNames() {
        return List.of();
    }

    default MetadataNode child(String name) {
        return null;
    }

    default void print(MetadataNodePrinter printer) {
        ArrayList<String> names = new ArrayList<>(childNames());
        names.sort(String::compareTo);
        for (String name : names) {
            printer.enterNode(name);
            MetadataNode child = child(name);
            if (child != null) 
                child.print(printer);
            printer.leaveNode();
        }
    }

    default String stringify() {
        NodeStringifier stringifier = new NodeStringifier();
        print(stringifier);
        return stringifier.toString();
    }
}
