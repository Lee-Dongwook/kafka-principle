package io.github.kafkaprinciple.shell.command;

import io.github.kafkaprinciple.shell.InteractiveShell;
import io.github.kafkaprinciple.shell.state.MetadataShellState;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.jline.reader.Candidate;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public final class Commands {
    public static final NavigableMap<String, Type> TYPES;

    static {
        TreeMap<String, Type> typesMap = new TreeMap<>();
        for (Type type : List.of(
            CatCommandHandler.TYPE,
            CdCommandHandler.TYPE,
            ExitCommandHandler.TYPE,
            FindCommandHandler.TYPE,
            HelpCommandHandler.TYPE,
            HistoryCommandHandler.TYPE,
            LsCommandHandler.TYPE,
            ManCommandHandler.TYPE,
            PwdCommandHandler.TYPE,
            TreeCommandHandler.TYPE
        )) {
            typesMap.put(type.name(), type);
        }
        TYPES = Collections.unmodifiableNavigableMap(typesMap);
    }

    public interface Handler {
        void run(
            Optional<InteractiveShell> shell,
            PrintWriter writer,
            MetadataShellState state
        ) throws Exception;
    }

    public interface Type {
        String name();
        String description();
        boolean shellOnly();
        void addArguments(ArgumentParser parser);
        Handler createHandler(Namespace namespace);
        void completeNext(
            MetadataShellState nodeManager,
            List<String> nextWords,
            List<Candidate> candidates
        ) throws Exception;
    }

    private final ArgumentParser parser;

    public Commands(boolean addShellCommands) {
        this.parser = ArgumentParsers.newArgumentParser("", false);
        Subparsers subparsers = this.parser.addSubparsers().dest("command");

        for (Type type : TYPES.values()) {
            if (addShellCommands || !type.shellOnly()) {
                Subparser subParser = subparsers.addParser(type.name());
                subParser.help(type.description());
                type.addArguments(subParser);
            }
        }
    }

    ArgumentParser parser() {
        return parser;
    }

    public Handler parseCommand(List<String> arguments) {
        List<String> trimmedArguments = new ArrayList<>(arguments);

        while(true) {
            if (trimmedArguments.isEmpty()) {
                return new NoOpCommandHandler();
            }

            String last = trimmedArguments.get(trimmedArguments.size() -1);

            if(!last.isEmpty()) {
                break;
            }

            trimmedArguments.remove(trimmedArguments.size() -1);
        }

        Namespace namespace;

        try {
            namespace = parser.parseArgs(trimmedArguments.toArray(new String[0]));
        } catch (HelpScreenException e) {
            return new NoOpCommandHandler();
        } catch (ArgumentParserException e) {
            return new ErroneousCommandHandler(e.getMessage());
        }

        String command = namespace.get("command");

        if(!command.equals(trimmedArguments.get(0))) {
            return new ErroneousCommandHandler("invalid choice: '" +
                trimmedArguments.get(0) + "': did you mean '" + command + "'?");
        }

        Type type = TYPES.get(command);

        if(type == null) {
            return new ErroneousCommandHandler("Unknown command specified: " + command);
        } else {
            return type.createHandler(namespace);
        }
    }
}
