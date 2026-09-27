package io.github.kafkaprinciple.common.config;

import io.github.kafkaprinciple.common.Configurable;
import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@InterfaceAudience.Public
public class AbstractConfig {
    private static final Logger log = LoggerFactory.getLogger(AbstractConfig.class);

    private final Set<String> used = ConcurrentHashMap.newKeySet();

    private final Map<String, ?> originals;

    private final Map<String, Object> values;

    private final ConfigDef definition;

    public static final String AUTOMATIC_CONFIG_PROVIDERS_PROPERTY = "io.github.kafkaprinciple.automatic.config.providers";

    public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";

    public static final String CONFIG_PROVIDERS_DOC = "Comma-separated alias names for classes implementing the <code>ConfigProvider</code> interface. "
            +
            "This enables loading configuration data (such as passwords, API keys, and other credentials) from external "
            +
            "sources.";

    private static final String CONFIG_PROVIDERS_PARAM = ".param.";

    @SuppressWarnings({ "this-escape" })
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps,
            boolean doLog) {
        Map<String, Object> originalMap = preProcessParsedConfig(
                Collections.unmodifiableMap(Utils.castToStringObjectMap(originals)));
        this.originals = resolveConfigVariables(configProviderProps, originalMap);
        this.values = definition.parse(this.originals);
        Map<String, Object> configUpdates = postProcessParsedConfig(Collections.unmodifiableMap(this.values));
        this.values.putAll(configUpdates);
        definition.parse(this.values);
        this.definition = definition;
        if (doLog)
            logAll();
    }

    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        this(definition, originals, Map.of(), true);
    }

    public AbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        this(definition, originals, Map.of(), doLog);
    }

    protected Map<String, Object> preProcessParsedConfig(Map<String, Object> parsedValues) {
        return parsedValues;
    }

    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        return Map.of();
    }

    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        used.add(key);
        return values.get(key);
    }

    public void ignore(String key) {
        used.add(key);
    }

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        final String res = (String) get(key);
        return res == null ? res : res.trim();
    }

    public ConfigDef.Type typeOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.type;
    }

    public String documentationOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.documentation;
    }

    public Password getPassword(String key) {
        return (Password) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public Set<String> unused() {
        Set<String> keys = new HashSet<>(originals.keySet());
        keys.removeAll(used);
        return keys;
    }

    public Map<String, Object> originals() {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        return copy;
    }

    public Map<String, Object> originals(Map<String, Object> configOverrides) {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        copy.putAll(configOverrides);
        return copy;
    }

    public Map<String, String> originalsStrings() {
        Map<String, String> copy = new RecordingMap<>();
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (!(entry.getValue() instanceof String))
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey() +
                        ": " + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
            copy.put(entry.getKey(), (String) entry.getValue());
        }
        return copy;
    }

    public Map<String, Object> originalsWithPrefix(String prefix) {
        return originalsWithPrefix(prefix, true);
    }

    public Map<String, Object> originalsWithPrefix(String prefix, boolean strip) {
        Map<String, Object> result = new RecordingMap<>(prefix, false);
        result.putAll(Utils.entriesWithPrefix(originals, prefix, strip));
        return result;
    }

    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                String keyWithNoPrefix = entry.getKey().substring(prefix.length());
                ConfigDef.ConfigKey configKey = definition.configKeys().get(keyWithNoPrefix);
                if (configKey != null)
                    result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                else {
                    String keyWithNoSecondaryPrefix = keyWithNoPrefix.substring(keyWithNoPrefix.indexOf('.') + 1);
                    configKey = definition.configKeys().get(keyWithNoSecondaryPrefix);
                    if (configKey != null)
                        result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                }
            }
        }
        return result;
    }

    public Map<String, Object> valuesWithPrefixAllOrNothing(String prefix) {
        Map<String, Object> withPrefix = originalsWithPrefix(prefix, true);

        if (withPrefix.isEmpty()) {
            return new RecordingMap<>(values(), "", true);
        } else {
            Map<String, Object> result = new RecordingMap<>(prefix, true);

            for (Map.Entry<String, ?> entry : withPrefix.entrySet()) {
                ConfigDef.ConfigKey configKey = definition.configKeys().get(entry.getKey());
                if (configKey != null)
                    result.put(entry.getKey(), definition.parseValue(configKey, entry.getValue(), true));
            }

            return result;
        }
    }

    public Map<String, ?> values() {
        return new RecordingMap<>(values);
    }

    public Map<String, ?> nonInternalValues() {
        Map<String, Object> nonInternalConfigs = new RecordingMap<>();
        values.forEach((key, value) -> {
            ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
            if (configKey == null || !configKey.internalConfig) {
                nonInternalConfigs.put(key, value);
            }
        });
        return nonInternalConfigs;
    }

    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);

        for (Map.Entry<String, Object> entry : new TreeMap<>(this.values).entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    public void logUnused() {
        Set<String> unusedKeys = unused();
        if (!unusedKeys.isEmpty()) {
            log.info("These configurations '{}' were supplied but are not used yet.", unusedKeys);
        }
    }

    private <T> T getConfiguredInstance(Object klass, Class<T> t, Map<String, Object> configPairs) {
        if (klass == null)
            return null;
        Object o;

        if (klass instanceof String) {
            try {
                o = Utils.newInstance((String) klass, t);
            } catch (ClassNotFoundException e) {
                throw new KafkaException("Class " + klass + " cannot be found", e);
            }
        } else if (klass instanceof Class<?>) {
            o = Utils.newInstance((Class<?>) klass);
        } else
            throw new KafkaException(
                    "Unexpected element of type " + klass.getClass().getName() + ", expected String or Class");
        try {
            if (!t.isInstance(o))
                throw new KafkaException(klass + " is not an instance of " + t.getName());
            if (o instanceof Configurable)
                ((Configurable) o).configure(configPairs);
        } catch (Exception e) {
            maybeClose(o,
                    "AutoCloseable object constructed and configured during failed call to getConfiguredInstance");
            throw e;
        }
        return t.cast(o);
    }

    public <T> T getConfiguredInstance(String key, Class<T> t) {
        return getConfiguredInstance(key, t, Map.of());
    }

    public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
        Class<?> c = getClass(key);

        return getConfiguredInstance(c, t, originals(configOverrides));
    }

    public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
        return getConfiguredInstances(key, t, Map.of());
    }

    public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
        return getConfiguredInstances(getList(key), t, configOverrides);
    }

    public <T> List<T> getConfiguredInstances(List<String> classNames, Class<T> t,
            Map<String, Object> configOverrides) {
        List<T> objects = new ArrayList<>();
        if (classNames == null)
            return objects;
        Map<String, Object> configPairs = originals();
        configPairs.putAll(configOverrides);

        try {
            for (Object klass : classNames) {
                Object o = getConfiguredInstance(klass, t, configPairs);
                objects.add(t.cast(o));
            }
        } catch (Exception e) {
            for (Object object : objects) {
                maybeClose(object,
                        "AutoCloseable object constructed and configured during failed call to getConfiguredInstances");
            }
            throw e;
        }
        return objects;
    }

    private static void maybeClose(Object object, String name) {
        if (object instanceof AutoCloseable) {
            Utils.closeQuietly((AutoCloseable) object, name);
        }
    }

    private Map<String, String> extractPotentialVariables(Map<?, ?> configMap) {
        Map<String, String> configMapAsString = new HashMap<>();
        for (Map.Entry<?, ?> entry : configMap.entrySet()) {
            if (entry.getValue() instanceof String)
                configMapAsString.put((String) entry.getKey(), (String) entry.getValue());
        }

        return configMapAsString;
    }

    private Map<String, ?> resolveConfigVariables(Map<String, ?> configProviderProps, Map<String, Object> originals) {
        Map<String, String> providerConfigString;
        Map<String, ?> configProperties;
        Predicate<String> classNameFilter;
        Map<String, Object> resolvedOriginals = new HashMap<>();
        Map<String, String> indirectVariables = extractPotentialVariables(originals);

        resolvedOriginals.putAll(originals);
        if (configProviderProps == null || configProviderProps.isEmpty()) {
            providerConfigString = indirectVariables;
            configProperties = originals;
            classNameFilter = automaticConfigProvidersFilter();
        } else {
            providerConfigString = extractPotentialVariables(configProviderProps);
            configProperties = configProviderProps;
            classNameFilter = ignored -> true;
        }
        Map<String, ConfigProvider> providers = instantiateConfigProviders(providerConfigString, configProperties,
                classNameFilter);

        if (!providers.isEmpty()) {
            ConfigTransformer configTransformer = new ConfigTransformer(providers);
            ConfigTransformerResult result = configTransformer.transform(indirectVariables);
            if (!result.data().isEmpty()) {
                resolvedOriginals.putAll(result.data());
            }
        }
        providers.values().forEach(x -> Utils.closeQuietly(x, "config provider"));

        return new ResolvingMap<>(resolvedOriginals, originals);
    }

    private Predicate<String> automaticConfigProvidersFilter() {
        String systemProperty = System.getProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY);
        if (systemProperty == null) {
            return ignored -> true;
        } else {
            return Arrays.stream(systemProperty.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet())::contains;
        }
    }

    private Map<String, Object> configProviderProperties(String configProviderPrefix,
            Map<String, ?> providerConfigProperties) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, ?> entry : providerConfigProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(configProviderPrefix) && key.length() > configProviderPrefix.length()) {
                result.put(key.substring(configProviderPrefix.length()), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, ConfigProvider> instantiateConfigProviders(
            Map<String, String> indirectConfigs,
            Map<String, ?> providerConfigProperties,
            Predicate<String> classNameFilter) {
        final String configProviders = indirectConfigs.get(CONFIG_PROVIDERS_CONFIG);

        if (configProviders == null || configProviders.isEmpty()) {
            return Map.of();
        }

        Map<String, String> providerMap = new HashMap<>();

        for (String provider : configProviders.split(",")) {
            String providerClass = providerClassProperty(provider);
            if (indirectConfigs.containsKey(providerClass)) {
                String providerClassName = indirectConfigs.get(providerClass);
                if (classNameFilter.test(providerClassName)) {
                    providerMap.put(provider, providerClassName);
                } else {
                    throw new ConfigException(providerClassName + " is not allowed. Update System property '"
                            + AUTOMATIC_CONFIG_PROVIDERS_PROPERTY + "' to allow " + providerClassName);
                }
            }
        }
        // Instantiate Config Providers
        Map<String, ConfigProvider> configProviderInstances = new HashMap<>();
        for (Map.Entry<String, String> entry : providerMap.entrySet()) {
            try {
                String prefix = CONFIG_PROVIDERS_CONFIG + "." + entry.getKey() + CONFIG_PROVIDERS_PARAM;
                Map<String, ?> configProperties = configProviderProperties(prefix, providerConfigProperties);
                ConfigProvider provider = Utils.newInstance(entry.getValue(), ConfigProvider.class);
                provider.configure(configProperties);
                configProviderInstances.put(entry.getKey(), provider);
            } catch (ClassNotFoundException e) {
                log.error("Could not load config provider class {}", entry.getValue(), e);
                throw new ConfigException(providerClassProperty(entry.getKey()), entry.getValue(),
                        "Could not load config provider class or one of its dependencies");
            }
        }

        return configProviderInstances;
    }

    private static String providerClassProperty(String providerName) {
        return String.format("%s.%s.class", CONFIG_PROVIDERS_CONFIG, providerName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AbstractConfig that = (AbstractConfig) o;

        return originals.equals(that.originals);
    }

    @Override
    public int hashCode() {
        return originals.hashCode();
    }

    private class RecordingMap<V> extends HashMap<String, V> {
        private final String prefix;
        private final boolean withIgnoreFallback;

        RecordingMap() {
            this("", false);
        }

        RecordingMap(String prefix, boolean withIgnoreFallback) {
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        RecordingMap(Map<String, ? extends V> m) {
            this(m, "", false);
        }

        RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
            super(m);
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        @Override
        public V get(Object key) {
            if (key instanceof String) {
                String stringKey = (String) key;
                String keyWithPrefix;
                if (prefix.isEmpty()) {
                    keyWithPrefix = stringKey;
                } else {
                    keyWithPrefix = prefix + stringKey;
                }
                ignore(keyWithPrefix);
                if (withIgnoreFallback)
                    ignore(stringKey);
            }
            return super.get(key);
        }
    }

    private static class ResolvingMap<V> extends HashMap<String, V> {

        private final Map<String, ?> originals;

        ResolvingMap(Map<String, ? extends V> resolved, Map<String, ?> originals) {
            super(resolved);
            this.originals = Collections.unmodifiableMap(originals);
        }

        @Override
        public V get(Object key) {
            if (key instanceof String && originals.containsKey(key)) {
                originals.get(key);
            }
            return super.get(key);
        }
    }
}
