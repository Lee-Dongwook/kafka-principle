package io.github.kafkaprinciple.shell.node;

import io.github.kafkaprinciple.image.node.MetadataNode;

/** Local shell-specific namespace. TODO: expose local shell metadata. */
public final class LocalShellNode implements MetadataNode {
    public static final String NAME = "local";
}
