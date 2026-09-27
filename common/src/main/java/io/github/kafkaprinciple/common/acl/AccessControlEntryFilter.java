package io.github.kafkaprinciple.common.acl;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public class AccessControlEntryFilter {
    private final AccessControlEntryData data;

    public static final AccessControlEntryFilter ANY = new AccessControlEntryFilter(null, null, AclOperation.ANY,
            AclPermissionType.ANY);

    public AccessControlEntryFilter(String principal, String host, AclOperation operation,
            AclPermissionType permissionType) {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(permissionType);
        this.data = new AccessControlEntryData(principal, host, operation, permissionType);
    }

    AccessControlEntryFilter(AccessControlEntryData data) {
        this.data = data;
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

    @Override
    public String toString() {
        return data.toString();
    }

    public boolean isUnknown() {
        return data.isUnknown();
    }

    public boolean matches(AccessControlEntry other) {
        if ((principal() != null) && (!principal().equals(other.principal())))
            return false;
        if ((host() != null) && (!host().equals(other.host())))
            return false;
        if ((operation() != AclOperation.ANY) && (!operation().equals(other.operation())))
            return false;
        return (permissionType() == AclPermissionType.ANY) || (permissionType().equals(other.permissionType()));
    }

    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    public String findIndefiniteField() {
        return data.findIndefiniteField();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryFilter))
            return false;
        AccessControlEntryFilter other = (AccessControlEntryFilter) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
