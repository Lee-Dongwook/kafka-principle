package io.github.kafkaprinciple.common.metrics;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public interface Stat {
    void record(MetricConfig config, double value, long timeMs);
}
