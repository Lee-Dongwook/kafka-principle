package io.github.kafkaprinciple.common.quota;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

@InterfaceAudience.Public
public class ClientQuotaFilter {
    private final Collection<ClientQuotaFilterComponent> components;
    private final boolean strict;

    private ClientQuotaFilter(Collection<ClientQuotaFilterComponent> components, boolean strict) {
        this.components = components;
        this.strict = strict;
    }

    public static ClientQuotaFilter contains(Collection<ClientQuotaFilterComponent> components) {
        return new ClientQuotaFilter(components, false);
    }

    public static ClientQuotaFilter containsOnly(Collection<ClientQuotaFilterComponent> components) {
        return new ClientQuotaFilter(components, true);
    }

    public static ClientQuotaFilter all() {
        return new ClientQuotaFilter(List.of(), false);
    }

    public Collection<ClientQuotaFilterComponent> components() {
        return this.components;
    }

    public boolean strict() {
        return this.strict;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClientQuotaFilter that = (ClientQuotaFilter) o;
        return Objects.equals(components, that.components) && Objects.equals(strict, that.strict);
    }

    @Override
    public int hashCode() {
        return Objects.hash(components, strict);
    }

    @Override
    public String toString() {
        return "ClientQuotaFilter(components=" + components + ", strict=" + strict + ")";
    }
}
