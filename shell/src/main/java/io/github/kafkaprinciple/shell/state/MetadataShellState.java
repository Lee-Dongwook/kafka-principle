package io.github.kafkaprinciple.shell.state;

import io.github.kafkaprinciple.image.MetadataImage;
import io.github.kafkaprinciple.image.node.MetadataNode;
import io.github.kafkapriniciple.shell.node.RootShellNode;

import java.util.function.Consumer;

public class MetadataShellState {
    private volatile MetadataNode root;
    private volatile String workingDirectory;

    public MetadataShellState() {
        this.root = new RootShellNode(MetadataImage.EMPTY);
        this.workingDirectory = "/";
    }

    public MetadataNode root() {
        return root;
    }

    public void setRoot(MetadataNode root) {
        this.root = root;
    }

    public String workingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public void visit(Consumer<MetadataShellState> consumer) {
        consumer.accept(this);
    }
}
