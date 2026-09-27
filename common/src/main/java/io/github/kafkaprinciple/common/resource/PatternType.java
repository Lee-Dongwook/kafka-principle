package io.github.kafkaprinciple.common.resource;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.function.Function;

@InterfaceAudience.Public
public enum PatternType {
    UNKNOWN((byte) 0),
    ANY((byte) 1),
    MATCH((byte) 2),
    LITERAL((byte) 3),
    PREFIXED((byte) 4);

    private static final Map<Byte, PatternType> CODE_TO_VALUE = Collections.unmodifiableMap(
            Arrays.stream(PatternType.values())
                    .collect(Collectors.toMap(PatternType::code, Function.identity())));

    private static final Map<String, PatternType> NAME_TO_VALUE = Collections.unmodifiableMap(
            Arrays.stream(PatternType.values())
                    .collect(Collectors.toMap(PatternType::name, Function.identity())));

    private final byte code;

    PatternType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    public boolean isSpecific() {
        return this != UNKNOWN && this != ANY && this != MATCH;
    }

    public static PatternType fromCode(byte code) {
        return CODE_TO_VALUE.getOrDefault(code, UNKNOWN);
    }

    public static PatternType fromString(String name) {
        return NAME_TO_VALUE.getOrDefault(name, UNKNOWN);
    }
}
