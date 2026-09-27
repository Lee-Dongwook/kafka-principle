package io.github.kafkaprinciple.common.resource;

import java.util.Objects;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class Resource {
    private final ResourceType resourceType;
    private final String name;

    public static final String CLUSTER_NAME = "kafka-cluster";
    public static final Resource CLUSTER = new Resource(ResourceType.CLUSTER, CLUSTER_NAME);

    public Resource(ResourceType resourceType, String name) {
        Objects.requireNonNull(resourceType);
        this.resourceType = resourceType;
        Objects.requireNonNull(name);
        this.name = name;
    }

    public ResourceType resourceType() {
        return resourceType;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + name + ")";
    }

    public boolean isUnknown() {
        return resourceType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Resource))
            return false;
        Resource other = (Resource) o;
        return resourceType.equals(other.resourceType) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name);
    }
}
