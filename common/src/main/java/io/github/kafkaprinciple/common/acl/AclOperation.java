package io.github.kafkaprinciple.common.acl;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.HashMap;
import java.util.Locale;

@InterfaceAudience.Public
public enum AclOperation {
    UNKNOWN((byte) 0),
    ANY((byte) 1),
    ALL((byte) 2),
    READ((byte) 3),
    WRITE((byte) 4),
    CREATE((byte) 5),
    DELETE((byte) 6),
    ALTER((byte) 7),
    DESCRIBE((byte) 8),
    CLUSTER_ACTION((byte) 9),
    DESCRIBE_CONFIGS((byte) 10),
    ALTER_CONFIGS((byte) 11),
    IDEMPOTENT_WRITE((byte) 12),
    CREATE_TOKENS((byte) 13),
    DESCRIBE_TOKENS((byte) 14),
    TWO_PHASE_COMMIT((byte) 15);

    private static final HashMap<Byte, AclOperation> CODE_TO_VALUE = new HashMap<>();

    static {
        for (AclOperation operation : AclOperation.values()) {
            CODE_TO_VALUE.put(operation.code, operation);
        }
    }

    public static AclOperation fromString(String str) throws IllegalArgumentException {
        try {
            return AclOperation.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static AclOperation fromCode(byte code) {
        AclOperation operation = CODE_TO_VALUE.get(code);
        if (operation == null) {
            return UNKNOWN;
        }
        return operation;
    }

    private final byte code;

    AclOperation(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
