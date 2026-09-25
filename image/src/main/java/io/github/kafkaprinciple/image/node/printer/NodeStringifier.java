package io.github.kafkaprinciple.image.node.printer;

import java.util.ArrayDeque;

public class NodeStringifier implements MetadataNodePrinter {
    private final MetadataNodeRedactionCriteria redactionCriteria;
    private final StringBuilder stringBuilder;
    private final ArrayDeque<String> prefixes;

    public NodeStringifier() {
        this(MetadataNodeRedactionCriteria.Strict.INSTANCE);
    }

    public NodeStringifier(MetadataNodeRedactionCriteria redactionCriteria) {
        this.redactionCriteria = redactionCriteria;
        this.stringBuilder = new StringBuilder();
        this.prefixes = new ArrayDeque<>();
        prefixes.push("");
    }

    @Override
    public MetadataNodeRedactionCriteria redactionCriteria() {
        return redactionCriteria;
    }

    @Override 
    public void enterNode(String name) {
        stringBuilder.append(prefixes.pop());
        prefixes.push(", ");
        stringBuilder.append(name).append("(");
        prefixes.push("");
    }

    @Override
    public void leaveNode() {
        stringBuilder.append(")");
        prefixes.pop();
    }

    @Override
    public void output(String text) {
        stringBuilder.append(prefixes.pop()).append(text);
        prefixes.push(", ");
    }

    @Override
    public String toString() {
        return stringBuilder.toString();
    }
}
