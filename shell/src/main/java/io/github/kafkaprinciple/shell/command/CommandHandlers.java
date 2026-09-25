package io.github.kafkaprinciple.shell.command;

import io.github.kafkaprinciple.shell.InteractiveShell;
import io.github.kafkaprinciple.shell.state.MetadataShellState;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

/**
 * Command skeletons required by {@link Commands}. Each handler is deliberately
 * non-functional until the corresponding shell command is implemented.
 */
abstract class PlaceholderCommandType implements Commands.Type {
    private final String name;
    private final String description;
    private final boolean shellOnly;

    PlaceholderCommandType(String name, String description, boolean shellOnly) {
        this.name = name;
        this.description = description;
        this.shellOnly = shellOnly;
    }

    @Override public String name() { return name; }
    @Override public String description() { return description; }
    @Override public boolean shellOnly() { return shellOnly; }
    @Override public void addArguments(ArgumentParser parser) { /* TODO */ }
    @Override public Commands.Handler createHandler(Namespace namespace) { return new NoOpCommandHandler(); }
    @Override public void completeNext(MetadataShellState state, List<String> nextWords, List<Candidate> candidates) { /* TODO */ }
}

final class CatCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new CatCommandHandler(); private CatCommandHandler() { super("cat", "TODO", false); } }
final class CdCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new CdCommandHandler(); private CdCommandHandler() { super("cd", "TODO", false); } }
final class ExitCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new ExitCommandHandler(); private ExitCommandHandler() { super("exit", "TODO", true); } }
final class FindCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new FindCommandHandler(); private FindCommandHandler() { super("find", "TODO", false); } }
final class HelpCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new HelpCommandHandler(); private HelpCommandHandler() { super("help", "TODO", true); } }
final class HistoryCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new HistoryCommandHandler(); private HistoryCommandHandler() { super("history", "TODO", true); } }
final class LsCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new LsCommandHandler(); private LsCommandHandler() { super("ls", "TODO", false); } }
final class ManCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new ManCommandHandler(); private ManCommandHandler() { super("man", "TODO", true); } }
final class PwdCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new PwdCommandHandler(); private PwdCommandHandler() { super("pwd", "TODO", false); } }
final class TreeCommandHandler extends PlaceholderCommandType { static final Commands.Type TYPE = new TreeCommandHandler(); private TreeCommandHandler() { super("tree", "TODO", false); } }

final class NoOpCommandHandler implements Commands.Handler {
    @Override public void run(Optional<InteractiveShell> shell, PrintWriter writer, MetadataShellState state) { /* TODO */ }
}

final class ErroneousCommandHandler implements Commands.Handler {
    private final String message;
    ErroneousCommandHandler(String message) { this.message = message; }
    @Override public void run(Optional<InteractiveShell> shell, PrintWriter writer, MetadataShellState state) { writer.println(message); }
}
