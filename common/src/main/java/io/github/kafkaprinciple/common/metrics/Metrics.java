package io.github.kafkaprinciple.common.metrics;

import io.github.kafkaprinciple.common.MetricName;
import io.github.kafkaprinciple.common.MetricNameTemplate;
import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.annotation.SuppressKafkaInternalApiUsage;
import io.github.kafkaprinciple.common.metrics.internals.MetricsUtils;
import io.github.kafkaprinciple.common.utils.Time;
import io.github.kafkaprinciple.common.utils.internals.KafkaThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

@InterfaceAudience.Public
public final class Metrics implements Closeable {
    private final MetricConfig config;
    private final ConcurrentMap<MetricName, KafkaMetric> metrics;
    private final ConcurrentMap<String, Sensor> sensors;
    private final ConcurrentMap<Sensor, List<Sensor>> childrenSensors;
    private final List<MetricsReporter> reporters;
    private final Time time;
    private final ScheduledThreadPoolExecutor metricsScheduler;
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    public Metrics() {
        this(new MetricConfig());
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(Time time) {
        this(new MetricConfig(), new ArrayList<>(0), time);
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(MetricConfig defaultConfig, Time time) {
        this(defaultConfig, new ArrayList<>(0), time);
    }

    public Metrics(MetricConfig defaultConfig) {
        this(defaultConfig, new ArrayList<>(0), Time.SYSTEM);
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        this(defaultConfig, reporters, time, false);
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time,
            MetricsContext metricsContext) {
        this(defaultConfig, reporters, time, false, metricsContext);
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration) {
        this(defaultConfig, reporters, time, enableExpiration, new KafkaMetricsContext(""));
    }

    @SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time for test injection — pending KIP review to promote Time or refactor")
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, boolean enableExpiration,
            MetricsContext metricsContext) {
        this.config = defaultConfig;
        this.sensors = new ConcurrentHashMap<>();
        this.metrics = new ConcurrentHashMap<>();
        this.childrenSensors = new ConcurrentHashMap<>();
        this.reporters = Objects.requireNonNull(reporters);
        this.time = time;
        for (MetricsReporter reporter : reporters) {
            reporter.contextChange(metricsContext);
            reporter.init(new ArrayList<>());
        }

        if (enableExpiration) {
            this.metricsScheduler = new ScheduledThreadPoolExecutor(1);
            this.metricsScheduler.setThreadFactory(runnable -> KafkaThread.daemon("SensorExpiryThread", runnable));
            this.metricsScheduler.scheduleAtFixedRate(new ExpireSensorTask(), 30, 30, TimeUnit.SECONDS);
        } else {
            this.metricsScheduler = null;
        }

        addMetric(metricName("count", "kafka-metrics-count", "total number of registered metrics"),
                (config, now) -> metrics.size());
    }

    public MetricName metricName(String name, String group, String description, Map<String, String> tags) {
        Map<String, String> combinedTag = new LinkedHashMap<>(config.tags());
        combinedTag.putAll(tags);
        return new MetricName(name, group, description, combinedTag);
    }

    public MetricName metricName(String name, String group, String description) {
        return metricName(name, group, description, new HashMap<>());
    }

    public MetricName metricName(String name, String group) {
        return metricName(name, group, "", new HashMap<>());
    }

    public MetricName metricName(String name, String group, String description, String... keyValue) {
        return metricName(name, group, description, MetricsUtils.getTags(keyValue));
    }

    public MetricName metricName(String name, String group, Map<String, String> tags) {
        return metricName(name, group, "", tags);
    }

    public static String toHtmlTable(String domain, Iterable<MetricNameTemplate> allMetrics) {
        Map<String, Map<String, String>> beansAndAttributes = new TreeMap<>();

        try (Metrics metrics = new Metrics()) {
            for (MetricNameTemplate template : allMetrics) {
                Map<String, String> tags = new LinkedHashMap<>();
                for (String s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }

                MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(),
                        tags);
                String mBeanName = JmxReporter.getMBeanName(domain, metricName);
                if (!beansAndAttributes.containsKey(mBeanName)) {
                    beansAndAttributes.put(mBeanName, new TreeMap<>());
                }
                Map<String, String> attrAndDesc = beansAndAttributes.get(mBeanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException(
                            "mBean '" + mBeanName + "' attribute '" + template.name() + "' is defined twice.");
                }
            }
        }
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\">\n<tbody>\n");
        b.append("<tr>\n");
        b.append("<th>Metric/Attribute name</th>\n");
        b.append("<th>Description</th>\n");
        b.append("<th>Mbean name</th>\n");
        b.append("</tr>\n");
        for (Entry<String, Map<String, String>> e : beansAndAttributes.entrySet()) {
            for (Entry<String, String> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>\n");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>\n");
                b.append("<td>");
                b.append(e.getKey());
                b.append("</td>\n");
                b.append("</tr>\n");
            }
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    public MetricConfig config() {
        return config;
    }

    public Sensor getSensor(String name) {
        return this.sensors.get(Objects.requireNonNull(name));
    }

    public Sensor sensor(String name) {
        return this.sensor(name, Sensor.RecordingLevel.INFO);
    }

    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel) {
        return sensor(name, null, recordingLevel, (Sensor[]) null);
    }

    public Sensor sensor(String name, Sensor... parents) {
        return this.sensor(name, Sensor.RecordingLevel.INFO, parents);
    }

    public Sensor sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, null, recordingLevel, parents);
    }

    public Sensor sensor(String name, MetricConfig config, Sensor... parents) {
        return this.sensor(name, config, Sensor.RecordingLevel.INFO, parents);
    }

    public Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds,
            Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        Sensor s = getSensor(name);
        if (s == null) {
            synchronized (this) {
                s = getSensor(name);
                if (s == null) {
                    s = new Sensor(this, name, parents, config == null ? this.config : config, time,
                            inactiveSensorExpirationTimeSeconds, recordingLevel);
                    this.sensors.put(name, s);
                    if (parents != null) {
                        for (Sensor parent : parents) {
                            List<Sensor> children = childrenSensors.computeIfAbsent(parent, k -> new ArrayList<>());
                            children.add(s);
                        }
                    }
                    log.trace("Added sensor with name {}", name);
                }
            }
        }
        return s;
    }

    public Sensor sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds,
            Sensor... parents) {
        return this.sensor(name, config, inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel.INFO, parents);
    }

    public void removeSensor(String name) {
        Sensor sensor = sensors.get(name);
        if (sensor != null) {
            List<Sensor> childSensors = null;
            synchronized (sensor) {
                synchronized (this) {
                    if (sensors.remove(name, sensor)) {
                        for (KafkaMetric metric : sensor.metrics())
                            removeMetric(metric.metricName());
                        log.trace("Removed sensor with name {}", name);
                        childSensors = childrenSensors.remove(sensor);
                        for (final Sensor parent : sensor.parents()) {
                            childrenSensors.getOrDefault(parent, emptyList()).remove(sensor);
                        }
                    }
                }
            }
            if (childSensors != null) {
                for (Sensor childSensor : childSensors)
                    removeSensor(childSensor.name());
            }
        }
    }

    public void addMetric(MetricName metricName, Measurable measurable) {
        addMetric(metricName, null, measurable);
    }

    public void addMetric(MetricName metricName, MetricConfig config, Measurable measurable) {
        addMetric(metricName, config, (MetricValueProvider<?>) measurable);
    }

    public void addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        KafkaMetric m = new KafkaMetric(new Object(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(metricValueProvider),
                config == null ? this.config : config,
                time);
        KafkaMetric existingMetric = registerMetric(m);
        if (existingMetric != null) {
            throw new IllegalArgumentException(
                    "A metric named '" + metricName + "' already exists, can't register another one.");
        }
    }

    public void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        addMetric(metricName, null, metricValueProvider);
    }

    public KafkaMetric addMetricIfAbsent(MetricName metricName, MetricConfig config,
            MetricValueProvider<?> metricValueProvider) {
        KafkaMetric metric = new KafkaMetric(new Object(),
                Objects.requireNonNull(metricName),
                Objects.requireNonNull(metricValueProvider),
                config == null ? this.config : config,
                time);

        KafkaMetric existingMetric = registerMetric(metric);
        return existingMetric == null ? metric : existingMetric;
    }

    public synchronized KafkaMetric removeMetric(MetricName metricName) {
        KafkaMetric metric = this.metrics.remove(metricName);
        if (metric != null) {
            for (MetricsReporter reporter : reporters) {
                try {
                    reporter.metricRemoval(metric);
                } catch (Exception e) {
                    log.error("Error when removing metric from {}", reporter.getClass().getName(), e);
                }
            }
            log.trace("Removed metric named {}", metricName);
        }
        return metric;
    }

    public synchronized void addReporter(MetricsReporter reporter) {
        Objects.requireNonNull(reporter).init(new ArrayList<>(metrics.values()));
        this.reporters.add(reporter);
    }

    public synchronized void removeReporter(MetricsReporter reporter) {
        if (this.reporters.remove(reporter)) {
            reporter.close();
        }
    }

    synchronized KafkaMetric registerMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        KafkaMetric existingMetric = this.metrics.putIfAbsent(metricName, metric);
        if (existingMetric != null) {
            return existingMetric;
        }
        for (MetricsReporter reporter : reporters) {
            try {
                reporter.metricChange(metric);
            } catch (Exception e) {
                log.error("Error when registering metric on {}", reporter.getClass().getName(), e);
            }
        }
        log.trace("Registered metric named {}", metricName);
        return null;
    }

    public Map<MetricName, KafkaMetric> metrics() {
        return this.metrics;
    }

    public List<MetricsReporter> reporters() {
        return this.reporters;
    }

    public KafkaMetric metric(MetricName metricName) {
        return this.metrics.get(metricName);
    }

    class ExpireSensorTask implements Runnable {
        @Override
        public void run() {
            for (Map.Entry<String, Sensor> sensorEntry : sensors.entrySet()) {
                synchronized (sensorEntry.getValue()) {
                    if (sensorEntry.getValue().hasExpired()) {
                        log.debug("Removing expired sensor {}", sensorEntry.getKey());
                        removeSensor(sensorEntry.getKey());
                    }
                }
            }
        }
    }

    Map<Sensor, List<Sensor>> childrenSensors() {
        return Collections.unmodifiableMap(childrenSensors);
    }

    public MetricName metricInstance(MetricNameTemplate template, String... keyValue) {
        return metricInstance(template, MetricsUtils.getTags(keyValue));
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        Set<String> runtimeTagKeys = new HashSet<>(tags.keySet());
        runtimeTagKeys.addAll(config().tags().keySet());

        Set<String> templateTagKeys = template.tags();

        if (!runtimeTagKeys.equals(templateTagKeys)) {
            throw new IllegalArgumentException(
                    "For '" + template.name() + "', runtime-defined metric tags do not match the tags in the template. "
                            + "Runtime = " + runtimeTagKeys + " Template = " + templateTagKeys.toString());
        }

        return this.metricName(template.name(), template.group(), template.description(), tags);
    }

    @Override
    public void close() {
        if (this.metricsScheduler != null) {
            this.metricsScheduler.shutdown();
            try {
                this.metricsScheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore and continue shutdown
                Thread.currentThread().interrupt();
            }
        }
        log.info("Metrics scheduler closed");

        for (MetricsReporter reporter : reporters) {
            try {
                log.info("Closing reporter {}", reporter.getClass().getName());
                reporter.close();
            } catch (Exception e) {
                log.error("Error when closing {}", reporter.getClass().getName(), e);
            }
        }
        log.info("Metrics reporters closed");
    }
}
