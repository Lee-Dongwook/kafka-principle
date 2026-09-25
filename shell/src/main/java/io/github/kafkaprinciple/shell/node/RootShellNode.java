package io.github.kafkaprinciple.shell.node;

import io.github.kafkaprinciple.image.MetadataImage;
import io.github.kafkaprinciple.image.node.MetadataImageNode;
import io.github.kafkaprinciple.image.node.MetadataNode;

import java.util.Collection;
import java.util.List;

public class RootShellNode implements MetadataNode {
    private final MetadataImage image;

    public RootShellNode(MetadataImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        return List.of(LocalShellNode.NAME, MetadataImageNode.NAME);
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals(LocalShellNode.NAME)) {
            return new LocalShellNode();
        } else if (name.equals(MetadataImageNode.NAME)) {
            return new MetadataImageNode(image);
        } else {
            return null;
        }
    }
}
