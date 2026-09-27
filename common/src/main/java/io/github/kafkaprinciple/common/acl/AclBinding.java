package io.github.kafkaprinciple.common.acl;

import java.util.Objects;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.resource.ResourcePattern;

@InterfaceAudience.Public
public class AclBinding {
    private final ResourcePattern pattern;
    private final AccessControlEntry entry;

    public AclBinding(ResourcePattern pattern, AccessControlEntry entry) {
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        this.entry = Objects.requireNonNull(entry, "entry");
    }

    public boolean isUnknown() {
        return pattern.isUnknown() || entry.isUnknown();
    }

    public ResourcePattern pattern() {
        return pattern;
    }

    public final AccessControlEntry entry() {
        return entry;
    }

    public AclBindingFilter toFilter() {
        return new AclBindingFilter(pattern.toFilter(), entry.toFilter());
    }

    @Override
    public String toString() {
        return "(pattern=" + pattern + ", entry=" + entry + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AclBinding that = (AclBinding) o;
        return Objects.equals(pattern, that.pattern) &&
                Objects.equals(entry, that.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, entry);
    }
}
