
package io.github.kafkaprinciple.message;

public final class IsNullConditional {
    interface ConditionalGenerator {
        String generate(String name, boolean negated);
    }

    private static class PrimitiveConditionalGenerator implements ConditionalGenerator {
        static final PrimitiveConditionalGenerator INSTANCE = new PrimitiveConditionalGenerator();

        @Override
        public String generate(String name, boolean negated) {
            if (negated) {
                return String.format("%s != null", name);
            } else {
                return String.format("%s == null", name);
            }
        }
    }

    static IsNullConditional forName(String name) {
        return new IsNullConditional(name);
    }

    static IsNullConditional forField(FieldSpec field) {
        IsNullConditional cond = new IsNullConditional(field.camelCaseName());
        cond.nullableVersions(field.nullableVersions());
        return cond;
    }

    private final String name;
    private Versions nullableVersions = Versions.ALL;
    private Versions possibleVersions = Versions.ALL;
    private Runnable ifNull = null;
    private Runnable ifShouldNotBeNull = null;
    private boolean alwaysEmitBlockScope = false;
    private ConditionalGenerator conditionalGenerator = PrimitiveConditionalGenerator.INSTANCE;

    private IsNullConditional(String name) {
        this.name = name;
    }

    IsNullConditional nullableVersions(Versions nullableVersions) {
        this.nullableVersions = nullableVersions;
        return this;
    }

    IsNullConditional possibleVersions(Versions possibleVersions) {
        this.possibleVersions = possibleVersions;
        return this;
    }

    IsNullConditional ifNull(Runnable ifNull) {
        this.ifNull = ifNull;
        return this;
    }

    IsNullConditional ifShouldNotBeNull(Runnable ifShouldNotBeNull) {
        this.ifShouldNotBeNull = ifShouldNotBeNull;
        return this;
    }

    IsNullConditional alwaysEmitBlockScope(boolean alwaysEmitBlockScope) {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope;
        return this;
    }

    IsNullConditional conditionalGenerator(ConditionalGenerator conditionalGenerator) {
        this.conditionalGenerator = conditionalGenerator;
        return this;
    }

    void generate(CodeBuffer buffer) {
        if (nullableVersions.intersect(possibleVersions).empty()) {
            if (ifShouldNotBeNull != null) {
                if (alwaysEmitBlockScope) {
                    buffer.printf("{%n");
                    buffer.incrementIndent();
                }
                ifShouldNotBeNull.run();
                if (alwaysEmitBlockScope) {
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                }
            }
        } else {
            if (ifNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, false));
                buffer.incrementIndent();
                ifNull.run();
                buffer.decrementIndent();
                if (ifShouldNotBeNull != null) {
                    buffer.printf("} else {%n");
                    buffer.incrementIndent();
                    ifShouldNotBeNull.run();
                    buffer.decrementIndent();
                }
                buffer.printf("}%n");
            } else if (ifShouldNotBeNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, true));
                buffer.incrementIndent();
                ifShouldNotBeNull.run();
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }
}
