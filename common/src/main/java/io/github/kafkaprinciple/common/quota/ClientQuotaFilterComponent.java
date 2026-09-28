package io.github.kafkaprinciple.common.quota;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;
import java.util.Optional;

@InterfaceAudience.Public
public class ClientQuotaFilterComponent {

    private final String entityType;
    private final Optional<String> match;

    private ClientQuotaFilterComponent(String entityType, Optional<String> match) {
        this.entityType = Objects.requireNonNull(entityType);
        this.match = match;
    }

    public static ClientQuotaFilterComponent ofEntity(String entityType, String entityName) {
        return new ClientQuotaFilterComponent(entityType, Optiona.of(Objects.requireNonNull(entityName)));
    }

    public static ClientQuotaFilterComponent ofDefaultEntity(String entityType) {
        return new ClientQuotaFilterComponent(entityType, Optional.empty());
    }

    public static ClientQuotaFilterComponent ofEntityType(String entityType) {
        return new ClientQuotaFilterComponent(entityType, null);
    }

    public String entityType() {
        return this.entityType;
    }

    public Optional<String> match() {
        return this.match;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClientQuotaFilterComponent that = (ClientQuotaFilterComponent) o;
        return Objects.equals(that.entityType, entityType) && Objects.equals(that.match, match);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityType, match);
    }

    @Override
    public String toString() {
        return "ClientQuotaFilterComponent(entityType=" + entityType + ", match=" + match + ")";
    }
}
