
package io.github.kafkaprinciple.message;

import java.util.Objects;
import java.util.TreeSet;

public final class HeaderGenerator {
    private static final String[] HEADER = new String[0];


    private final CodeBuffer buffer;

    private final TreeSet<String> imports;
    private final String packageName;

    private final TreeSet<String> staticImports;

    public HeaderGenerator(String packageName) {
        this.buffer = new CodeBuffer();
        this.imports = new TreeSet<>();
        this.packageName = packageName;
        this.staticImports = new TreeSet<>();
    }

    public void addImport(String newImport) {
        this.imports.add(newImport);
    }

    public void addStaticImport(String newImport) {
        this.staticImports.add(newImport);
    }

    public void generate() {
        Objects.requireNonNull(packageName);
        for (String header : HEADER) {
            buffer.printf("%s%n", header);
        }
        buffer.printf("package %s;%n", packageName);
        buffer.printf("%n");
        for (String newImport : imports) {
            buffer.printf("import %s;%n", newImport);
        }
        buffer.printf("%n");
        if (!staticImports.isEmpty()) {
            for (String newImport : staticImports) {
                buffer.printf("import static %s;%n", newImport);
            }
            buffer.printf("%n");
        }
    }

    public CodeBuffer buffer() {
        return buffer;
    }
}
