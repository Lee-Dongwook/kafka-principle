package io.github.kafkaprinciple.common.acl;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.HashMap;
import java.util.Locale;

@InterfaceAudience.Public
public enum AclPermissionType {
    UNKNOWN((byte) 0),
    ANY((byte) 1),
    DENY((byte) 2),
    ALLOW((byte) 3);

    private static final HashMap<Byte, AclPermissionType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (AclPermissionType permissionType : AclPermissionType.values()) {
            CODE_TO_VALUE.put(permissionType.code, permissionType);
        }
    }

    public static AclPermissionType fromString(String str) {
        try {
            return AclPermissionType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static AclPermissionType fromCode(byte code) {
        AclPermissionType permissionType = CODE_TO_VALUE.get(code);
        if (permissionType == null) {
            return UNKNOWN;
        }
        return permissionType;
    }

    private final byte code;

    AclPermissionType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
