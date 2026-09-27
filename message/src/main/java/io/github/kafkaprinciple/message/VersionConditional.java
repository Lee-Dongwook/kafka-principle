
package io.github.kafkaprinciple.message;

public final class VersionConditional {
        static VersionConditional forVersions(Versions containingVersions,
                                          Versions possibleVersions) {
        return new VersionConditional(containingVersions, possibleVersions);
    }

    private final Versions containingVersions;
    private final Versions possibleVersions;
    private ClauseGenerator ifMember = null;
    private ClauseGenerator ifNotMember = null;
    private boolean alwaysEmitBlockScope = false;
    private boolean allowMembershipCheckAlwaysFalse = true;

    private VersionConditional(Versions containingVersions, Versions possibleVersions) {
        this.containingVersions = containingVersions;
        this.possibleVersions = possibleVersions;
    }

    VersionConditional ifMember(ClauseGenerator ifMember) {
        this.ifMember = ifMember;
        return this;
    }

    VersionConditional ifNotMember(ClauseGenerator ifNotMember) {
        this.ifNotMember = ifNotMember;
        return this;
    }

        VersionConditional alwaysEmitBlockScope(boolean alwaysEmitBlockScope) {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope;
        return this;
    }

        VersionConditional allowMembershipCheckAlwaysFalse(boolean allowMembershipCheckAlwaysFalse) {
        this.allowMembershipCheckAlwaysFalse = allowMembershipCheckAlwaysFalse;
        return this;
    }

    private void generateFullRangeCheck(Versions ifVersions,
                                        Versions ifNotVersions,
                                        CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if ((_version >= %d) && (_version <= %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if ((_version < %d) || (_version > %d)) {%n",
                    containingVersions.lowest(), containingVersions.highest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateLowerRangeCheck(Versions ifVersions,
                                         Versions ifNotVersions,
                                         CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (_version >= %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if (_version < %d) {%n", containingVersions.lowest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateUpperRangeCheck(Versions ifVersions,
                                         Versions ifNotVersions,
                                         CodeBuffer buffer) {
        if (ifMember != null) {
            buffer.printf("if (_version <= %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            ifMember.generate(ifVersions);
            buffer.decrementIndent();
            if (ifNotMember != null) {
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                ifNotMember.generate(ifNotVersions);
                buffer.decrementIndent();
            }
            buffer.printf("}%n");
        } else if (ifNotMember != null) {
            buffer.printf("if (_version > %d) {%n", containingVersions.highest());
            buffer.incrementIndent();
            ifNotMember.generate(ifNotVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateAlwaysTrueCheck(Versions ifVersions, CodeBuffer buffer) {
        if (ifMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            ifMember.generate(ifVersions);
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    private void generateAlwaysFalseCheck(Versions ifNotVersions, CodeBuffer buffer) {
        if (!allowMembershipCheckAlwaysFalse) {
            throw new RuntimeException("Version ranges " + containingVersions +
                " and " + possibleVersions + " have no versions in common.");
        }
        if (ifNotMember != null) {
            if (alwaysEmitBlockScope) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            ifNotMember.generate(ifNotVersions);
            if (alwaysEmitBlockScope) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    void generate(CodeBuffer buffer) {
        Versions ifVersions = possibleVersions.intersect(containingVersions);
        Versions ifNotVersions = possibleVersions.subtract(containingVersions);
        if (ifNotVersions == null) {
            ifNotVersions = possibleVersions;
        }

        if (possibleVersions.lowest() < containingVersions.lowest()) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateFullRangeCheck(ifVersions, ifNotVersions, buffer);
            } else if (possibleVersions.highest() >= containingVersions.lowest()) {
                generateLowerRangeCheck(ifVersions, ifNotVersions, buffer);
            } else {
                generateAlwaysFalseCheck(ifNotVersions, buffer);
            }
        } else if (possibleVersions.highest() >= containingVersions.lowest() &&
                    (possibleVersions.lowest() <= containingVersions.highest())) {
            if (possibleVersions.highest() > containingVersions.highest()) {
                generateUpperRangeCheck(ifVersions, ifNotVersions, buffer);
            } else {
                generateAlwaysTrueCheck(ifVersions, buffer);
            }
        } else {
            generateAlwaysFalseCheck(ifNotVersions, buffer);
        }
    }
}
