package io.github.kafkaprinciple.common.acl;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public class AccessControlEntry {
    final AccessControlEntryData data;

    public AccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType) {
        Objects.requireNonNull(principal);
        Objects.requireNonNull(host);
        Objects.requireNonNull(operation);
        if (operation == AclOperation.ANY)
            throw new IllegalArgumentException("operation must not be ANY");
        Objects.requireNonNull(permissionType);
        if (permissionType == AclPermissionType.ANY)
            throw new IllegalArgumentException("permissionType must not be ANY");
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    public String principal() {
        return data.principal();
    }

    public String host() {
        return data.host();
    }

    public AclOperation operation() {
        return data.operation();
    }

    public AclPermissionType permissionType() {
        return data.permissionType();
    }

    public AccessControlEntryFilter toFilter() {
        return new AccessControlEntryFilter(data);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    public boolean isUnknown() {
        return data.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntry))
            return false;
        AccessControlEntry other = (AccessControlEntry) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
