package io.github.kafkaprinciple.common.metrics.stats;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.metrics.MeasurableStat;
import io.github.kafkaprinciple.common.metrics.MetricConfig;
import io.github.kafkaprinciple.common.metrics.Quota;

import java.util.concurrent.TimeUnit;

import static io.github.kafkaprinciple.common.metrics.internals.MetricsUtils.convert;

@InterfaceAudience.Public
public class TokenBucket implements MeasurableStat {
    private final TimeUnit unit;
    private double tokens;
    private long lastUpdateMs;

    public TokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public TokenBucket(TimeUnit unit) {
        this.unit = unit;
        this.tokens = 0;
        this.lastUpdateMs = 0;
    }

    @Override
    public double measure(final MetricConfig config, final long timeMs) {
        if (config.quota() == null)
            return Long.MAX_VALUE;
        final double quota = config.quota().bound();
        final double burst = burst(config);
        refill(quota, burst, timeMs);
        return this.tokens;
    }

    @Override
    public void record(final MetricConfig config, final double value, final long timeMs) {
        if (config.quota() == null)
            return;
        final double quota = config.quota().bound();
        final double burst = burst(config);
        refill(quota, burst, timeMs);
        this.tokens = Math.min(burst, this.tokens - value);
    }

    private void refill(final double quota, final double burst, final long timeMs) {
        this.tokens = Math.min(burst, this.tokens + quota * convert(timeMs - lastUpdateMs, unit));
        this.lastUpdateMs = timeMs;
    }

    private double burst(final MetricConfig config) {
        return config.samples() * convert(config.timeWindowMs(), unit) * config.quota().bound();
    }

    @Override
    public String toString() {
        return "TokenBucket(" +
                "unit=" + unit +
                ", tokens=" + tokens +
                ", lastUpdateMs=" + lastUpdateMs +
                ')';
    }
}
