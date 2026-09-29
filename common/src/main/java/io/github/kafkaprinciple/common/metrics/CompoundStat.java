package io.github.kafkaprinciple.common.metrics;

import io.github.kafkaprinciple.common.MetricName;
import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.List;

@InterfaceAudience.Public
public interface CompoundStat extends Stat {
    List<NamedMeasurable> stats();

    class NamedMeasurable {
        private final MetricName name;
        private final Measurable stat;

        public NamedMeasurable(MetricName name, Measurable stat) {
            super();
            this.name = name;
            this.stat = stat;
        }

        public MetricName name() {
            return name;
        }

        public Measurable stat() {
            return stat;
        }
    }
}
