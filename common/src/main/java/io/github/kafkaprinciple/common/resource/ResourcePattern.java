package io.github.kafkaprinciple.common.resource;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public class ResourcePattern {
    public static final String WILDCARD_RESOURCE = "*";

    private final ResourceType resourceType;
    private final String name;
    private final PatternType patternType;

    public ResourcePattern(ResourceType resourceType, String name, PatternType patternType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = Objects.requireNonNull(name, "name");
        this.patternType = Objects.requireNonNull(patternType, "patternType");

        if (resourceType == ResourceType.ANY) {
            throw new IllegalArgumentException("resourceType must not be ANY");
        }

        if (patternType == PatternType.MATCH || patternType == PatternType.ANY) {
            throw new IllegalArgumentException("patternType must not be " + patternType);
        }
    }

    public ResourceType resourceType() {
        return resourceType;
    }

    public String name() {
        return name;
    }

    public PatternType patternType() {
        return patternType;
    }

    public ResourcePatternFilter toFilter() {
        return new ResourcePatternFilter(resourceType, name, patternType);
    }

    @Override
    public String toString() {
        return "ResourcePattern(resourceType=" + resourceType + ", name=" + name + ", patternType=" + patternType + ")";
    }

    public boolean isUnknown() {
        return resourceType.isUnknown() || patternType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourcePattern resource = (ResourcePattern) o;
        return resourceType == resource.resourceType &&
                Objects.equals(name, resource.name) &&
                patternType == resource.patternType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, patternType);
    }
}
