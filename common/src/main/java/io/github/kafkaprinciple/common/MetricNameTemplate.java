package io.github.kafkaprinciple.common;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

@InterfaceAudience.Public
public class MetricNameTemplate {
    private final String name;
    private final String group;
    private final String description;
    private final LinkedHashSet<String> tags;

    public MetricNameTemplate(String name, String group, String description, Set<String> tagsNames) {
        this.name = Objects.requireNonNull(name);
        this.group = Objects.requireNonNull(group);
        this.description = Objects.requireNonNull(description);
        this.tags = new LinkedHashSet<>(Objects.requireNonNull(tagsNames));
    }

    public MetricNameTemplate(String name, String group, String description, String... tagsNames) {
        this(name, group, description, getTags(tagsNames));
    }

    private static LinkedHashSet<String> getTags(String... keys) {
        LinkedHashSet<String> tags = new LinkedHashSet<>();

        Collections.addAll(tags, keys);

        return tags;
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public String description() {
        return this.description;
    }

    public Set<String> tags() {
        return tags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, group, tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetricNameTemplate other = (MetricNameTemplate) o;
        return Objects.equals(name, other.name) && Objects.equals(group, other.group) &&
                Objects.equals(tags, other.tags);
    }

    @Override
    public String toString() {
        return String.format("name=%s, group=%s, tags=%s", name, group, tags);
    }
}
