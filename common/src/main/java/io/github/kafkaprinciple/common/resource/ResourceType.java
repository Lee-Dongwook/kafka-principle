package io.github.kafkaprinciple.common.resource;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.HashMap;
import java.util.Locale;

@InterfaceAudience.Public
public enum ResourceType {
    UNKNOWN((byte) 0),
    ANY((byte) 1),
    TOPIC((byte) 2),
    GROUP((byte) 3),
    CLUSTER((byte) 4),
    TRANSACTIONAL_ID((byte) 5),
    DELEGATION_TOKEN((byte) 6),
    USER((byte) 7);

    private static final HashMap<Byte, ResourceType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (ResourceType resourceType : ResourceType.values()) {
            CODE_TO_VALUE.put(resourceType.code, resourceType);
        }
    }

    public static ResourceType fromString(String str) throws IllegalArgumentException {
        try {
            return ResourceType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static ResourceType fromCode(byte code) {
        ResourceType resourceType = CODE_TO_VALUE.get(code);
        if (resourceType == null) {
            return UNKNOWN;
        }
        return resourceType;
    }

    private final byte code;

    ResourceType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
