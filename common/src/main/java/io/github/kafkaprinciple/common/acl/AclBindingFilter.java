package io.github.kafkaprinciple.common.acl;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.resource.ResourcePatternFilter;

import java.util.Objects;

@InterfaceAudience.Public
public class AclBindingFilter {
    private final ResourcePatternFilter patternFilter;
    private final AccessControlEntryFilter entryFilter;

    public static final AclBindingFilter ANY = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            AccessControlEntryFilter.ANY);

    public AclBindingFilter(ResourcePatternFilter patternFilter, AccessControlEntryFilter entryFilter) {
        this.patternFilter = Objects.requireNonNull(patternFilter, "patternFilter");
        this.entryFilter = Objects.requireNonNull(entryFilter, "entryFilter");
    }

    public boolean isUnknown() {
        return patternFilter.isUnknown() || entryFilter.isUnknown();
    }

    public ResourcePatternFilter patternFilter() {
        return patternFilter;
    }

    public final AccessControlEntryFilter entryFilter() {
        return entryFilter;
    }

    @Override
    public String toString() {
        return "(patternFilter=" + patternFilter + ", entryFilter=" + entryFilter + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AclBindingFilter that = (AclBindingFilter) o;
        return Objects.equals(patternFilter, that.patternFilter) &&
                Objects.equals(entryFilter, that.entryFilter);
    }

    public boolean matchesAtMostOne() {
        return patternFilter.matchesAtMostOne() && entryFilter.matchesAtMostOne();
    }

    public String findIndefiniteField() {
        String indefinite = patternFilter.findIndefiniteField();
        if (indefinite != null)
            return indefinite;
        return entryFilter.findIndefiniteField();
    }

    public boolean matches(AclBinding binding) {
        return patternFilter.matches(binding.pattern()) && entryFilter.matches(binding.entry());
    }

    @Override
    public int hashCode() {
        return Objects.hash(patternFilter, entryFilter);
    }
}
