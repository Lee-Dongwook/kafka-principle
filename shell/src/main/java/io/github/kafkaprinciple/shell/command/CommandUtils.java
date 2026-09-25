package io.github.kafkaprinciple.shell.command;

import org.jline.reader.Candidate;

import java.util.List;

/** Command completion helpers. TODO: add argument-aware completion. */
public final class CommandUtils {
    private CommandUtils() { }

    public static void completeCommand(String prefix, List<Candidate> candidates) {
        Commands.TYPES.keySet().stream()
            .filter(name -> name.startsWith(prefix))
            .forEach(name -> candidates.add(new Candidate(name)));
    }
}
