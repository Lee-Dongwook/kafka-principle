
package io.github.kafkaprinciple.message;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

public class CodeBuffer {
    private final ArrayList<String> lines;
    private int indent;

    public CodeBuffer() {
        this.lines = new ArrayList<>();
        this.indent = 0;
    }

    public void incrementIndent() {
        indent++;
    }

    public void decrementIndent() {
        indent--;
        if (indent < 0) {
            throw new RuntimeException("Indent < 0");
        }
    }

    public void printf(String format, Object... args) {
        lines.add(String.format(indentSpaces() + format, args));
    }

    public void write(Writer writer) throws IOException {
        for (String line : lines) {
            writer.write(line);
        }
    }

    public void write(CodeBuffer other) {
        for (String line : lines) {
            other.lines.add(other.indentSpaces() + line);
        }
    }

    private String indentSpaces() {
        return "    ".repeat(Math.max(0, indent));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CodeBuffer)) {
            return false;
        }
        CodeBuffer o = (CodeBuffer) other;
        return lines.equals(o.lines);
    }

    @Override
    public int hashCode() {
        return lines.hashCode();
    }
}
