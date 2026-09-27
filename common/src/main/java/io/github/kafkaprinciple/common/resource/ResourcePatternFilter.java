package io.github.kafkaprinciple.common.resource;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

import static io.github.kafkaprinciple.common.resource.ResourcePattern.WILDCARD_RESOURCE;

@InterfaceAudience.Public
public class ResourcePatternFilter {
    public static final ResourcePatternFilter ANY = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);

    private final ResourceType resourceType;
    private final String name;
    private final PatternType patternType;

    public ResourcePatternFilter(ResourceType resourceType, String name, PatternType patternType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = name;
        this.patternType = Objects.requireNonNull(patternType, "patternType");
    }

    public boolean isUnknown() {
        return resourceType.isUnknown() || patternType.isUnknown();
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

    public boolean matches(ResourcePattern pattern) {
        if (!resourceType.equals(ResourceType.ANY) && !resourceType.equals(pattern.resourceType())) {
            return false;
        }

        if (!patternType.equals(PatternType.ANY) && !patternType.equals(PatternType.MATCH)
                && !patternType.equals(pattern.patternType())) {
            return false;
        }

        if (name == null) {
            return true;
        }

        if (patternType.equals(PatternType.ANY) || patternType.equals(pattern.patternType())) {
            return name.equals(pattern.name());
        }

        switch (pattern.patternType()) {
            case LITERAL:
                return name.equals(pattern.name()) || pattern.name().equals(WILDCARD_RESOURCE);

            case PREFIXED:
                return name.startsWith(pattern.name());

            default:
                throw new IllegalArgumentException("Unsupported PatternType: " + pattern.patternType());
        }
    }

    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    public String findIndefiniteField() {
        if (resourceType == ResourceType.ANY)
            return "Resource type is ANY.";
        if (resourceType == ResourceType.UNKNOWN)
            return "Resource type is UNKNOWN.";
        if (name == null)
            return "Resource name is NULL.";
        if (patternType == PatternType.MATCH)
            return "Resource pattern type is MATCH.";
        if (patternType == PatternType.UNKNOWN)
            return "Resource pattern type is UNKNOWN.";
        return null;
    }

    @Override
    public String toString() {
        return "ResourcePattern(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name)
                + ", patternType=" + patternType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourcePatternFilter resource = (ResourcePatternFilter) o;
        return resourceType == resource.resourceType &&
                Objects.equals(name, resource.name) &&
                patternType == resource.patternType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, patternType);
    }
}
