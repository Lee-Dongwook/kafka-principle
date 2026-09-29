package io.github.kafkaprinciple.common.metrics;

import io.github.kafkaprinciple.common.MetricName;
import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.metrics.CompoundStat.NamedMeasurable;
import io.github.kafkaprinciple.common.metrics.stats.TokenBucket;
import io.github.kafkaprinciple.common.utils.Time;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

@InterfaceAudience.Public
public final class Sensor {
    private final Metrics registry;
    private final String name;
    private final Sensor[] parents;
    private final List<StatAndConfig> stats;
    private final Map<MetricName, KafkaMetric> metrics;
    private final MetricConfig config;
    private final Time time;
    private volatile long lastRecordTime;
    private final long inactiveSensorExpirationTimeMs;
    private final Object metricLock;

    private static class StatAndConfig {
        private final Stat stat;
        private final Supplier<MetricConfig> configSupplier;

        StatAndConfig(Stat stat, Supplier<MetricConfig> configSupplier) {
            this.stat = stat;
            this.configSupplier = configSupplier;
        }

        public Stat stat() {
            return stat;
        }

        public MetricConfig config() {
            return configSupplier.get();
        }

        @Override
        public String toString() {
            return "StatAndConfig(stat=" + stat + ')';
        }
    }

    public enum RecordingLevel {
        INFO(0, "INFO"), DEBUG(1, "DEBUG"), TRACE(2, "TRACE");

        private static final RecordingLevel[] ID_TO_TYPE;
        private static final int MIN_RECORDING_LEVEL_KEY = 0;
        public static final int MAX_RECORDING_LEVEL_KEY;

        static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
            RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        public final String name;
        public final short id;

        RecordingLevel(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        public static RecordingLevel forId(int id) {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
                throw new IllegalArgumentException(
                        String.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                                "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            return ID_TO_TYPE[id];
        }

        public static RecordingLevel forName(String name) {
            return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public boolean shouldRecord(final int configId) {
            if (configId == INFO.id) {
                return this.id == INFO.id;
            } else if (configId == DEBUG.id) {
                return this.id == INFO.id || this.id == DEBUG.id;
            } else if (configId == TRACE.id) {
                return true;
            } else {
                throw new IllegalStateException("Did not recognize recording level " + configId);
            }
        }
    }

    private final RecordingLevel recordingLevel;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time,
            long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel) {
        super();
        this.registry = registry;
        this.name = Objects.requireNonNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new LinkedHashMap<>();
        this.stats = new ArrayList<>();
        this.config = config;
        this.time = time;
        this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds,
                TimeUnit.SECONDS);
        this.lastRecordTime = time.milliseconds();
        this.recordingLevel = recordingLevel;
        this.metricLock = new Object();
        checkForest(new HashSet<>());
    }

    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (Sensor parent : parents)
            parent.checkForest(sensors);
    }

    public String name() {
        return this.name;
    }

    List<Sensor> parents() {
        return unmodifiableList(asList(parents));
    }

    public boolean shouldRecord() {
        return this.recordingLevel.shouldRecord(config.recordLevel().id);
    }

    public void record() {
        if (shouldRecord()) {
            recordInternal(1.0d, time.milliseconds(), true);
        }
    }

    public void record(double value) {
        if (shouldRecord()) {
            recordInternal(value, time.milliseconds(), true);
        }
    }

    public void record(double value, long timeMs) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, true);
        }
    }

    public void record(double value, long timeMs, boolean checkQuotas) {
        if (shouldRecord()) {
            recordInternal(value, timeMs, checkQuotas);
        }
    }

    private void recordInternal(double value, long timeMs, boolean checkQuotas) {
        this.lastRecordTime = timeMs;
        synchronized (this) {
            synchronized (metricLock()) {
                for (StatAndConfig statAndConfig : this.stats) {
                    statAndConfig.stat.record(statAndConfig.config(), value, timeMs);
                }
            }
            if (checkQuotas)
                checkQuotas(timeMs);
        }
        for (Sensor parent : parents)
            parent.record(value, timeMs, checkQuotas);
    }

    public void checkQuotas() {
        checkQuotas(time.milliseconds());
    }

    public void checkQuotas(long timeMs) {
        for (KafkaMetric metric : this.metrics.values()) {
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    double value = metric.measurableValue(timeMs);
                    if (metric.measurable() instanceof TokenBucket) {
                        if (value < 0) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    } else {
                        if (!quota.acceptable(value)) {
                            throw new QuotaViolationException(metric, value, quota.bound());
                        }
                    }
                }
            }
        }
    }

    public boolean add(CompoundStat stat) {
        return add(stat, null);
    }

    public synchronized boolean add(CompoundStat stat, MetricConfig config) {
        if (hasExpired())
            return false;

        final MetricConfig statConfig = config == null ? this.config : config;
        stats.add(new StatAndConfig(Objects.requireNonNull(stat), () -> statConfig));
        Object lock = metricLock();
        for (NamedMeasurable m : stat.stats()) {
            final KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), statConfig, time);
            if (!metrics.containsKey(metric.metricName())) {
                KafkaMetric existingMetric = registry.registerMetric(metric);
                if (existingMetric != null) {
                    throw new IllegalArgumentException(
                            "A metric named '" + metric.metricName() + "' already exists, can't register another one.");
                }
                metrics.put(metric.metricName(), metric);
            }
        }
        return true;
    }

    public boolean add(MetricName metricName, MeasurableStat stat) {
        return add(metricName, stat, null);
    }

    public synchronized boolean add(final MetricName metricName, final MeasurableStat stat, final MetricConfig config) {
        if (hasExpired()) {
            return false;
        } else if (metrics.containsKey(metricName)) {
            return true;
        } else {
            final MetricConfig statConfig = config == null ? this.config : config;
            final KafkaMetric metric = new KafkaMetric(
                    metricLock(),
                    Objects.requireNonNull(metricName),
                    Objects.requireNonNull(stat),
                    statConfig,
                    time);
            KafkaMetric existingMetric = registry.registerMetric(metric);
            if (existingMetric != null) {
                throw new IllegalArgumentException(
                        "A metric named '" + metricName + "' already exists, can't register another one.");
            }
            metrics.put(metric.metricName(), metric);
            stats.add(new StatAndConfig(Objects.requireNonNull(stat), metric::config));
            return true;
        }
    }

    public synchronized boolean hasMetrics() {
        return !metrics.isEmpty();
    }

    public boolean hasExpired() {
        return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
    }

    synchronized List<KafkaMetric> metrics() {
        return List.copyOf(this.metrics.values());
    }

    private Object metricLock() {
        return metricLock;
    }
}
