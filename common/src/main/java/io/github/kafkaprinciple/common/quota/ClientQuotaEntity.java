package io.github.kafkaprinciple.common.quota;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Map;
import java.util.Objects;

@InterfaceAudience.Public
public class ClientQuotaEntity {
    private final Map<String, String> entries;

    public static final String USER = "user";
    public static final String CLIENT_ID = "client-id";
    public static final String IP = "ip";

    public static boolean isValidEntityType(String entityType) {
        return Objects.equals(entityType, USER) ||
                Objects.equals(entityType, CLIENT_ID) ||
                Objects.equals(entityType, IP);
    }

    public ClientQuotaEntity(Map<String, String> entries) {
        this.entries = entries;
    }

    public Map<String, String> entries() {
        return this.entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClientQuotaEntity that = (ClientQuotaEntity) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    @Override
    public String toString() {
        return "ClientQuotaEntity(entries=" + entries + ")";
    }
}
