package io.github.kafkaprinciple.common.acl;

import java.util.Objects;

class AccessControlEntryData {
    private final String principal;
    private final String host;
    private final AclOperation operation;
    private final AclPermissionType permissionType;

    AccessControlEntryData(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        this.principal = principal;
        this.host = host;
        this.operation = operation;
        this.permissionType = permissionType;
    }

    String principal() {
        return principal;
    }

    String host() {
        return host;
    }

    AclOperation operation() {
        return operation;
    }

    AclPermissionType permissionType() {
        return permissionType;
    }

    public String findIndefiniteField() {
        if (principal() == null)
            return "Principal is NULL";
        if (host() == null)
            return "Host is NULL";
        if (operation() == AclOperation.ANY)
            return "Operation is ANY";
        if (operation() == AclOperation.UNKNOWN)
            return "Operation is UNKNOWN";
        if (permissionType() == AclPermissionType.ANY)
            return "Permission type is ANY";
        if (permissionType() == AclPermissionType.UNKNOWN)
            return "Permission type is UNKNOWN";
        return null;
    }

    @Override
    public String toString() {
        return "(principal=" + (principal == null ? "<any>" : principal) +
                ", host=" + (host == null ? "<any>" : host) +
                ", operation=" + operation +
                ", permissionType=" + permissionType + ")";
    }

    boolean isUnknown() {
        return operation.isUnknown() || permissionType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryData))
            return false;
        AccessControlEntryData other = (AccessControlEntryData) o;
        return Objects.equals(principal, other.principal) &&
                Objects.equals(host, other.host) &&
                Objects.equals(operation, other.operation) &&
                Objects.equals(permissionType, other.permissionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, host, operation, permissionType);
    }
}
