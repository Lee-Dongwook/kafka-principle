package io.github.kafkaprinciple.common.config;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.config.types.Password;
import io.github.kafkaprinciple.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConfigDef {
    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDef.class);

    public static final Object NO_DEFAULT_VALUE = new Object();

    private final Map<String, ConfigKey> configKeys;
    private final List<String> groups;
    private Set<String> configsWithNoParent;

    public ConfigDef() {
        configKeys = new LinkedHashMap<>();
        groups = new LinkedList<>();
        configsWithNoParent = null;
    }

    public ConfigDef(ConfigDef base) {
        configKeys = new LinkedHashMap<>(base.configKeys);
        groups = new LinkedList<>(base.groups);
        configsWithNoParent = null;
    }

    public Set<String> names() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    public Map<String, Object> defaultValues() {
        Map<String, Object> defaultValues = new HashMap<>();

        for (ConfigKey key : configKeys.values()) {
            if (key.defaultValue != NO_DEFAULT_VALUE)
                defaultValues.put(key.name, key.defaultValue);
        }
        return defaultValues;
    }

    public ConfigDef define(ConfigKey key) {
        if (configKeys.containsKey(key.name)) {
            throw new ConfigException("Configuration " + key.name + " is defined twice.");
        }
        if (key.group != null && !groups.contains(key.group)) {
            groups.add(key.group);
        }
        configKeys.put(key.name, key);
        return this;
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
             String group, int orderInGroup, Width width, String displayName, List<String> dependents,
             Recommender recommender) {
         return define(new ConfigKey(name, type, defaultValue, validator, importance, documentation, group,
                 orderInGroup, width, displayName, dependents, recommender, false, null));
    }
     
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender,
                            String alternativeString) {
        return define(new ConfigKey(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender, false, alternativeString));
    }

    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                displayName, List.of(), recommender);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, List.of());
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
            String group, int orderInGroup, Width width, String displayName, List<String> dependents,
            Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width,
                displayName, dependents, recommender);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width,
                displayName, dependents, null);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width,
                displayName, List.of(), recommender);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width,
                displayName, List.of());
    }
    
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
            Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                displayName, dependents, recommender);
    }
    
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, List.of(), recommender);
    }

    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
            Width width, String displayName) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width,
                displayName, List.of());
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance,
            String documentation) {
        return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }
    
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
            String alternativeString) {
        return define(name, type, defaultValue, null, importance, documentation, null, -1, Width.NONE,
                name, List.of(), null, alternativeString);
    }
    
    public ConfigDef define(String name, Type type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
    }

    public ConfigDef defineInternal(final String name, final Type type, final Object defaultValue,
            final Importance importance) {
        return define(new ConfigKey(name, type, defaultValue, null, importance, "", "", -1, Width.NONE, name, List.of(),
                null, true, null));
    }

    public ConfigDef defineInternal(final String name, final Type type, final Object defaultValue, final Validator validator, final Importance importance, final String documentation) {
        return define(new ConfigKey(name, type, defaultValue, validator, importance, documentation, "", -1, Width.NONE, name, List.of(), null, true, null));
    }
    
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    public List<String> groups() {
        return groups;
    }

    public ConfigDef withClientSslSupport() {
        SslConfigs.addClientSslSupport(this);
        return this;
    }

    public ConfigDef withClientSaslSupport() {
        SaslConfigs.addClientSaslSupport(this);
        return this;
    }
    
    public Map<String, Object> parse(Map<?, ?> props) {
        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        if (!undefinedConfigKeys.isEmpty()) {
            String joined = undefinedConfigKeys.stream().map(String::toString).collect(Collectors.joining(","));
            throw new ConfigException(
                    "Some configurations in are referred in the dependents, but not defined: " + joined);
        }

        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values())
            values.put(key.name, parseValue(key, props.get(key.name), props.containsKey(key.name)));
        return values;
    }
    
    Object parseValue(ConfigKey key, Object value, boolean isSet) {
        Object parsedValue;
        if (isSet) {
            parsedValue = parseType(key.name, value, key.type);
        } else if (NO_DEFAULT_VALUE.equals(key.defaultValue)) {
            throw new ConfigException(
                    "Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            parsedValue = key.defaultValue;
        }
        if (key.validator instanceof ValidList && parsedValue instanceof List) {
            List<?> originalListValue = (List<?>) parsedValue;
            parsedValue = originalListValue.stream().distinct().collect(Collectors.toList());
            if (originalListValue.size() != ((List<?>) parsedValue).size()) {
                LOGGER.warn(
                        "Configuration key \"{}\" contains duplicate values. Duplicates will be removed. The original value "
                                +
                                "is: {}, the updated value is: {}",
                        key.name, originalListValue, parsedValue);
            }
        }
        if (key.validator != null) {
            key.validator.ensureValid(key.name, parsedValue);
        }
        return parsedValue;
    }
    
    public List<ConfigValue> validate(Map<String, String> props) {
        return new ArrayList<>(validateAll(props).values());
    }

    public Map<String, ConfigValue> validateAll(Map<String, String> props) {
        Map<String, ConfigValue> configValues = new HashMap<>();
        for (String name : configKeys.keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        for (String undefinedConfigKey : undefinedConfigKeys) {
            ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
            undefinedConfigValue
                    .addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValues.put(undefinedConfigKey, undefinedConfigValue);
        }

        Map<String, Object> parsed = parseForValidate(props, configValues);
        return validate(parsed, configValues);
    }
    
    Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
        Map<String, Object> parsed = new HashMap<>();
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            parseForValidate(name, props, parsed, configValues);
        }
        return parsed;
    }
    
    private Map<String, ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name : configsWithNoParent) {
            validate(name, parsed, configValues);
        }
        return configValues;
    }
    

    private List<String> undefinedDependentConfigs() {
        Set<String> undefinedConfigKeys = new HashSet<>();
        for (ConfigKey configKey : configKeys.values()) {
            for (String dependent : configKey.dependents) {
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent);
                }
            }
        }
        return new ArrayList<>(undefinedConfigKeys);
    }
    
    Set<String> getConfigsWithNoParent() {
        if (this.configsWithNoParent != null) {
            return this.configsWithNoParent;
        }
        Set<String> configsWithParent = new HashSet<>();

        for (ConfigKey configKey : configKeys.values()) {
            List<String> dependents = configKey.dependents;
            configsWithParent.addAll(dependents);
        }

        Set<String> configs = new HashSet<>(configKeys.keySet());
        configs.removeAll(configsWithParent);
        this.configsWithNoParent = configs;
        return configs;
    }
    
    private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed,
            Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);

        Object value = null;
        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props.get(key.name), key.type);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        } else if (NO_DEFAULT_VALUE.equals(key.defaultValue)) {
            config.addErrorMessage("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            value = key.defaultValue;
        }

        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }
        config.value(value);
        parsed.put(name, value);
        for (String dependent : key.dependents) {
            parseForValidate(dependent, props, parsed, configs);
        }
    }
    
    private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue value = configs.get(name);
        if (key.recommender != null) {
            try {
                List<Object> recommendedValues = key.recommender.validValues(name, parsed);
                List<Object> originalRecommendedValues = value.recommendedValues();
                if (!originalRecommendedValues.isEmpty()) {
                    Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
                    recommendedValues.removeIf(o -> !originalRecommendedValueSet.contains(o));
                }
                value.recommendedValues(recommendedValues);
                value.visible(key.recommender.visible(name, parsed));
            } catch (ConfigException e) {
                value.addErrorMessage(e.getMessage());
            }
        }

        configs.put(name, value);
        for (String dependent : key.dependents) {
            validate(dependent, parsed, configs);
        }
    }
    
    public static Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) return null;

            String trimmed = null;
            if (value instanceof String)
                trimmed = ((String) value).trim();

            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true"))
                            return true;
                        else if (trimmed.equalsIgnoreCase("false"))
                            return false;
                        else
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                    } else if (value instanceof Boolean)
                        return value;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                case PASSWORD:
                    if (value instanceof Password)
                        return value;
                    else if (value instanceof String)
                        return new Password(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case STRING:
                    if (value instanceof String)
                        return trimmed;
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case INT:
                    if (value instanceof Integer) {
                        return value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 32-bit integer, but it was a " + value.getClass().getName());
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be a 16-bit integer (short), but it was a " + value.getClass().getName());
                    }
                case LONG:
                    if (value instanceof Integer)
                        return ((Integer) value).longValue();
                    if (value instanceof Long)
                        return value;
                    else if (value instanceof String)
                        return Long.parseLong(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a 64-bit integer (long), but it was a " + value.getClass().getName());
                case DOUBLE:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof String)
                        return Double.parseDouble(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a double, but it was a " + value.getClass().getName());
                case LIST:
                    if (value instanceof List)
                        return value;
                    else if (value instanceof String)
                        if (trimmed.isEmpty())
                            return List.of();
                        else
                            return Arrays.asList(COMMA_WITH_WHITESPACE.split(trimmed, -1));
                    else
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                case CLASS:
                    if (value instanceof Class)
                        return value;
                    else if (value instanceof String) {
                        return Utils.loadClass(trimmed, Object.class);
                    } else
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    public static String convertToString(Object parsedValue, Type type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case STRING:
            case PASSWORD:
                return parsedValue.toString();
            case LIST:
                List<?> valueList = (List<?>) parsedValue;
                return valueList.stream().map(Object::toString).collect(Collectors.joining(","));
            case CLASS:
                Class<?> clazz = (Class<?>) parsedValue;
                return clazz.getName();
            default:
                throw new IllegalStateException("Unknown type.");
        }
    }

    public static Map<String, String> convertToStringMapWithPasswordValues(Map<String, ?> configs) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            Object value = entry.getValue();
            String strValue;
            if (value instanceof Password)
                strValue = ((Password) value).value();
            else if (value instanceof List)
                strValue = convertToString(value, Type.LIST);
            else if (value instanceof Class)
                strValue = convertToString(value, Type.CLASS);
            else
                strValue = convertToString(value, null);
            if (strValue != null)
                result.put(entry.getKey(), strValue);
        }
        return result;
    }
    
    public enum Type {
        BOOLEAN,
        STRING,
        INT,
        SHORT,
        LONG,
        DOUBLE,
        LIST,
        CLASS,
        PASSWORD;

        public boolean isSensitive() {
            return this == PASSWORD;
        }
    }

    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    public enum Width {
        NONE, SHORT, MEDIUM, LONG
    }

    public interface Recommender {
        List<Object> validValues(String name, Map<String, Object> parsedConfig);

        boolean visible(String name, Map<String, Object> parsedConfig);
    }

    public interface Validator {
        void ensureValid(String name, Object value);
    }

    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new ConfigException(name, null, "Value must be non-null");
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue())
                throw new ConfigException(name, o, "Value must be at least " + min);
            if (max != null && n.doubleValue() > max.doubleValue())
                throw new ConfigException(name, o, "Value must be no more than " + max);
        }

        public String toString() {
            if (min == null && max == null)
                return "[...]";
            else if (min == null)
                return "[...," + max + "]";
            else if (max == null)
                return "[" + min + ",...]";
            else
                return "[" + min + ",...," + max + "]";
        }
    }

    public static class ValidList implements Validator {

        final ValidString validString;
        final boolean isEmptyAllowed;
        final boolean isNullAllowed;

        private ValidList(List<String> validStrings, boolean isEmptyAllowed, boolean isNullAllowed) {
            this.validString = new ValidString(validStrings);
            this.isEmptyAllowed = isEmptyAllowed;
            this.isNullAllowed = isNullAllowed;
        }

        public static ValidList anyNonDuplicateValues(boolean isEmptyAllowed, boolean isNullAllowed) {
            return new ValidList(List.of(), isEmptyAllowed, isNullAllowed);
        }

        public static ValidList in(String... validStrings) {
            return new ValidList(List.of(validStrings), true, false);
        }

        public static ValidList in(boolean isEmptyAllowed, String... validStrings) {
            if (!isEmptyAllowed && validStrings.length == 0) {
                throw new IllegalArgumentException(
                        "At least one valid string must be provided when empty values are not allowed");
            }
            return new ValidList(List.of(validStrings), isEmptyAllowed, false);
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                if (isNullAllowed)
                    return;
                else
                    throw new ConfigException("Configuration '" + name + "' values must not be null.");
            }

            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) value;
            if (!isEmptyAllowed && values.isEmpty()) {
                String validString = this.validString.validStrings.isEmpty() ? "any non-empty value"
                        : this.validString.toString();
                throw new ConfigException(
                        "Configuration '" + name + "' must not be empty. Valid values include: " + validString);
            }

            if (Set.copyOf(values).size() != values.size()) {
                throw new ConfigException("Configuration '" + name + "' values must not be duplicated.");
            }

            validateIndividualValues(name, values);
        }

        private void validateIndividualValues(String name, List<Object> values) {
            boolean hasValidStrings = !validString.validStrings.isEmpty();

            for (Object value : values) {
                if (value instanceof String) {
                    String string = (String) value;
                    if (string.isEmpty()) {
                        throw new ConfigException("Configuration '" + name + "' values must not be empty.");
                    }
                    if (hasValidStrings) {
                        validString.ensureValid(name, value);
                    }
                }
            }
        }

        public String toString() {
            return !validString.validStrings.isEmpty() ? validString.toString() : "";
        }
    }
    
    public static class ValidString implements Validator {
        final List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + String.join(", ", validStrings));
            }

        }

        public String toString() {
            return "[" + String.join(", ", validStrings) + "]";
        }
    }

    public static class CaseInsensitiveValidString implements Validator {

        final Set<String> validStrings;

        private CaseInsensitiveValidString(List<String> validStrings) {
            this.validStrings = validStrings.stream()
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());
        }

        public static CaseInsensitiveValidString in(String... validStrings) {
            return new CaseInsensitiveValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
                throw new ConfigException(name, o, "String must be one of (case insensitive): " + String.join(", ", validStrings));
            }
        }

        public String toString() {
            return "(case insensitive) [" + String.join(", ", validStrings) + "]";
        }
    }

    public static class NonNullValidator implements Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, "null", "entry must be non null");
            }
        }

        public String toString() {
            return "non-null string";
        }
    }

    public static class LambdaValidator implements Validator {
        BiConsumer<String, Object> ensureValid;
        Supplier<String> toStringFunction;

        private LambdaValidator(BiConsumer<String, Object> ensureValid,
                                Supplier<String> toStringFunction) {
            this.ensureValid = ensureValid;
            this.toStringFunction = toStringFunction;
        }

        public static LambdaValidator with(BiConsumer<String, Object> ensureValid,
                                           Supplier<String> toStringFunction) {
            return new LambdaValidator(ensureValid, toStringFunction);
        }

        @Override
        public void ensureValid(String name, Object value) {
            ensureValid.accept(name, value);
        }

        @Override
        public String toString() {
            return toStringFunction.get();
        }
    }

    public static class CompositeValidator implements Validator {
        private final List<Validator> validators;

        private CompositeValidator(List<Validator> validators) {
            this.validators = Collections.unmodifiableList(validators);
        }

        public static CompositeValidator of(Validator... validators) {
            return new CompositeValidator(Arrays.asList(validators));
        }

        @Override
        public void ensureValid(String name, Object value) {
            for (Validator validator: validators) {
                validator.ensureValid(name, value);
            }
        }

        @Override
        public String toString() {
            if (validators == null) return "";
            StringBuilder desc = new StringBuilder();
            for (Validator v: validators) {
                if (desc.length() > 0) {
                    desc.append(',').append(' ');
                }
                desc.append(v);
            }
            return desc.toString();
        }
    }

    public static class NonEmptyString implements Validator {

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s != null && s.isEmpty()) {
                throw new ConfigException(name, o, "String must be non-empty");
            }
        }

        @Override
        public String toString() {
            return "non-empty string";
        }
    }
    
    public static class NonEmptyStringWithoutControlChars implements Validator {

        public static NonEmptyStringWithoutControlChars nonEmptyStringWithoutControlChars() {
            return new NonEmptyStringWithoutControlChars();
        }

        @Override
        public void ensureValid(String name, Object value) {
            String s = (String) value;

            if (s == null) {
                return;
            } else if (s.isEmpty()) {
                throw new ConfigException(name, value, "String may not be empty");
            }

            ArrayList<Integer> foundIllegalCharacters = new ArrayList<>();

            for (int i = 0; i < s.length(); i++) {
                if (Character.isISOControl(s.codePointAt(i))) {
                    foundIllegalCharacters.add(s.codePointAt(i));
                }
            }

            if (!foundIllegalCharacters.isEmpty()) {
                throw new ConfigException(name, value, "String may not contain control sequences but had the following ASCII chars: " +
                        foundIllegalCharacters.stream().map(Object::toString).collect(Collectors.joining(", ")));
            }
        }

        public String toString() {
            return "non-empty string without ISO control characters";
        }
    }

    public static class ListSize implements Validator {
        final int maxSize;

        private ListSize(final int maxSize) {
            this.maxSize = maxSize;
        }

        public static ListSize atMostOfSize(final int maxSize) {
            return new ListSize(maxSize);
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            @SuppressWarnings("unchecked")
            List<String> values = (List<String>) value;
            if (values.size() > maxSize) {
                throw new ConfigException(name, value, "exceeds maximum list size of [" + maxSize + "].");
            }
        }

        @Override
        public String toString() {
            return "List containing maximum of " + maxSize + " elements";
        }
    }

    public static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;
        public final String group;
        public final int orderInGroup;
        public final Width width;
        public final String displayName;
        public final List<String> dependents;
        public final Recommender recommender;
        public final boolean internalConfig;
        public final String alternativeString;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator,
                Importance importance, String documentation, String group,
                int orderInGroup, Width width, String displayName,
                List<String> dependents, Recommender recommender,
                boolean internalConfig) {
            this(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width,
                    displayName,
                    dependents, recommender, internalConfig, null);
        }

        private ConfigKey(String name, Type type, Object defaultValue, Validator validator,
                Importance importance, String documentation, String group,
                int orderInGroup, Width width, String displayName,
                List<String> dependents, Recommender recommender,
                boolean internalConfig, String alternativeString) {
            this.name = name;
            this.type = type;
            boolean hasDefault = !NO_DEFAULT_VALUE.equals(defaultValue);
            this.defaultValue = hasDefault ? parseType(name, defaultValue, type) : NO_DEFAULT_VALUE;
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && hasDefault)
                this.validator.ensureValid(name, this.defaultValue);
            this.documentation = documentation;
            this.dependents = dependents;
            this.group = group;
            this.orderInGroup = orderInGroup;
            this.width = width;
            this.displayName = displayName;
            this.recommender = recommender;
            this.internalConfig = internalConfig;
            this.alternativeString = alternativeString;
        }

        public boolean hasDefault() {
            return !NO_DEFAULT_VALUE.equals(this.defaultValue);
        }

        public Type type() {
            return type;
        }
    }
    
    protected List<String> headers() {
        return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", "Importance");
    }

    protected String getConfigValue(ConfigKey key, String headerName) {
        switch (headerName) {
            case "Name":
                return key.name;
            case "Description":
                return key.documentation;
            case "Type":
                return key.type.toString().toLowerCase(Locale.ROOT);
            case "Default":
                if (key.hasDefault()) {
                    if (key.defaultValue == null)
                        return "null";
                    String defaultValueStr = convertToString(key.defaultValue, key.type);
                    if (defaultValueStr.isEmpty())
                        return "\"\"";
                    else {
                        String suffix = "";
                        if (key.name.endsWith(".bytes")) {
                            suffix = niceMemoryUnits(((Number) key.defaultValue).longValue());
                        } else if (key.name.endsWith(".ms")) {
                            suffix = niceTimeUnits(((Number) key.defaultValue).longValue());
                        }
                        return defaultValueStr + suffix;
                    }
                } else
                    return "";
            case "Valid Values":
                return key.validator != null ? key.validator.toString() : "";
            case "Importance":
                return key.importance.toString().toLowerCase(Locale.ROOT);
            default:
                throw new RuntimeException("Can't find value for header '" + headerName + "' in " + key.name);
        }
    }

    static String niceMemoryUnits(long bytes) {
        long value = bytes;
        int i = 0;
        while (value != 0 && i < 4) {
            if (value % 1024L == 0) {
                value /= 1024L;
                i++;
            } else {
                break;
            }
        }
        String resultFormat = " (" + value + " %s" + (value == 1 ? ")" : "s)");
        switch (i) {
            case 1:
                return String.format(resultFormat, "kibibyte");
            case 2:
                return String.format(resultFormat, "mebibyte");
            case 3:
                return String.format(resultFormat, "gibibyte");
            case 4:
                return String.format(resultFormat, "tebibyte");
            default:
                return "";
        }
    }

    static String niceTimeUnits(long millis) {
        long value = millis;
        long[] divisors = { 1000, 60, 60, 24 };
        String[] units = { "second", "minute", "hour", "day" };
        int i = 0;
        while (value != 0 && i < 4) {
            if (value % divisors[i] == 0) {
                value /= divisors[i];
                i++;
            } else {
                break;
            }
        }
        if (i > 0) {
            return " (" + value + " " + units[i - 1] + (value > 1 ? "s)" : ")");
        }
        return "";
    }
    
    public String toHtmlTable() {
        return toHtmlTable(Map.of());
    }

    private void addHeader(StringBuilder builder, String headerName) {
        builder.append("<th>");
        builder.append(headerName);
        builder.append("</th>\n");
    }

    private void addColumnValue(StringBuilder builder, String value) {
        builder.append("<td>");
        builder.append(value);
        builder.append("</td>");
    }

    public String toHtmlTable(Map<String, String> dynamicUpdateModes) {
        boolean hasUpdateModes = !dynamicUpdateModes.isEmpty();
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        // print column headers
        for (String headerName : headers()) {
            addHeader(b, headerName);
        }
        if (hasUpdateModes)
            addHeader(b, "Dynamic Update Mode");
        b.append("</tr>\n");
        for (ConfigKey key : configs) {
            if (key.internalConfig) {
                continue;
            }
            b.append("<tr>\n");
            // print column values
            for (String headerName : headers()) {
                addColumnValue(b, getConfigValue(key, headerName));
                b.append("</td>");
            }
            if (hasUpdateModes) {
                String updateMode = dynamicUpdateModes.get(key.name);
                if (updateMode == null)
                    updateMode = "read-only";
                addColumnValue(b, updateMode);
            }
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    public String toRst() {
        StringBuilder b = new StringBuilder();
        for (ConfigKey key : sortedConfigs()) {
            if (key.internalConfig) {
                continue;
            }
            getConfigKeyRst(key, b);
            b.append("\n");
        }
        return b.toString();
    }

    public String toEnrichedRst() {
        StringBuilder b = new StringBuilder();

        String lastKeyGroupName = "";
        for (ConfigKey key : sortedConfigs()) {
            if (key.internalConfig) {
                continue;
            }
            if (key.group != null) {
                if (!lastKeyGroupName.equalsIgnoreCase(key.group)) {
                    b.append(key.group).append("\n");

                    char[] underLine = new char[key.group.length()];
                    Arrays.fill(underLine, '^');
                    b.append(new String(underLine)).append("\n\n");
                }
                lastKeyGroupName = key.group;
            }

            getConfigKeyRst(key, b);

            if (key.dependents != null && key.dependents.size() > 0) {
                int j = 0;
                b.append("  * Dependents: ");
                for (String dependent : key.dependents) {
                    b.append("``");
                    b.append(dependent);
                    if (++j == key.dependents.size())
                        b.append("``");
                    else
                        b.append("``, ");
                }
                b.append("\n");
            }
            b.append("\n");
        }
        return b.toString();
    }

    private void getConfigKeyRst(ConfigKey key, StringBuilder b) {
        b.append("``").append(key.name).append("``").append("\n");
        if (key.documentation != null) {
            for (String docLine : key.documentation.split("\n")) {
                if (docLine.isEmpty()) {
                    continue;
                }
                b.append("  ").append(docLine).append("\n\n");
            }
        } else {
            b.append("\n");
        }
        b.append("  * Type: ").append(getConfigValue(key, "Type")).append("\n");
        if (key.hasDefault()) {
            b.append("  * Default: ").append(getConfigValue(key, "Default")).append("\n");
        }
        if (key.validator != null) {
            b.append("  * Valid Values: ").append(getConfigValue(key, "Valid Values")).append("\n");
        }
        b.append("  * Importance: ").append(getConfigValue(key, "Importance")).append("\n");
    }

    private List<ConfigKey> sortedConfigs() {
        final Map<String, Integer> groupOrd = new HashMap<>(groups.size());
        int ord = 0;
        for (String group : groups) {
            groupOrd.put(group, ord++);
        }

        List<ConfigKey> configs = new ArrayList<>(configKeys.values());
        configs.sort((k1, k2) -> compare(k1, k2, groupOrd));
        return configs;
    }
    
    private int compare(ConfigKey k1, ConfigKey k2, Map<String, Integer> groupOrd) {
        int cmp = k1.group == null
                ? (k2.group == null ? 0 : -1)
                : (k2.group == null ? 1 : Integer.compare(groupOrd.get(k1.group), groupOrd.get(k2.group)));
        if (cmp == 0) {
            cmp = Integer.compare(k1.orderInGroup, k2.orderInGroup);
            if (cmp == 0) {
                if (!k1.hasDefault() && k2.hasDefault())
                    cmp = -1;
                else if (!k2.hasDefault() && k1.hasDefault())
                    cmp = 1;
                else {
                    cmp = k1.importance.compareTo(k2.importance);
                    if (cmp == 0)
                        return k1.name.compareTo(k2.name);
                }
            }
        }
        return cmp;
    }
    
    public void embed(final String keyPrefix, final String groupPrefix, final int startingOrd, final ConfigDef child) {
        int orderInGroup = startingOrd;
        for (ConfigKey key : child.sortedConfigs()) {
            define(new ConfigKey(
                    keyPrefix + key.name,
                    key.type,
                    key.defaultValue,
                    embeddedValidator(keyPrefix, key.validator),
                    key.importance,
                    key.documentation,
                    groupPrefix + (key.group == null ? "" : ": " + key.group),
                    orderInGroup++,
                    key.width,
                    key.displayName,
                    embeddedDependents(keyPrefix, key.dependents),
                    embeddedRecommender(keyPrefix, key.recommender),
                    key.internalConfig,
                    key.alternativeString));
        }
    }

    private static Validator embeddedValidator(final String keyPrefix, final Validator base) {
        if (base == null)
            return null;
        return ConfigDef.LambdaValidator.with(
                (name, value) -> base.ensureValid(name.substring(keyPrefix.length()), value), base::toString);
    }
    
    private static List<String> embeddedDependents(final String keyPrefix, final List<String> dependents) {
        if (dependents == null)
            return null;
        final List<String> updatedDependents = new ArrayList<>(dependents.size());
        for (String dependent : dependents) {
            updatedDependents.add(keyPrefix + dependent);
        }
        return updatedDependents;
    }
    
    private static Recommender embeddedRecommender(final String keyPrefix, final Recommender base) {
        if (base == null)
            return null;
        return new Recommender() {
            private String unprefixed(String k) {
                return k.substring(keyPrefix.length());
            }

            private Map<String, Object> unprefixed(Map<String, Object> parsedConfig) {
                final Map<String, Object> unprefixedParsedConfig = new HashMap<>(parsedConfig.size());
                for (Map.Entry<String, Object> e : parsedConfig.entrySet()) {
                    if (e.getKey().startsWith(keyPrefix)) {
                        unprefixedParsedConfig.put(unprefixed(e.getKey()), e.getValue());
                    }
                }
                return unprefixedParsedConfig;
            }

            @Override
            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                return base.validValues(unprefixed(name), unprefixed(parsedConfig));
            }

            @Override
            public boolean visible(String name, Map<String, Object> parsedConfig) {
                return base.visible(unprefixed(name), unprefixed(parsedConfig));
            }
        };
    }
    
    public String toHtml() {
        return toHtml(Map.of());
    }

    public String toHtml(int headerDepth, Function<String, String> idGenerator) {
        return toHtml(headerDepth, idGenerator, Map.of());
    }

    public String toHtml(Map<String, String> dynamicUpdateModes) {
        return toHtml(4, Function.identity(), dynamicUpdateModes);
    }

    public String toHtml(int headerDepth, Function<String, String> idGenerator,
            Map<String, String> dynamicUpdateModes) {
        boolean hasUpdateModes = !dynamicUpdateModes.isEmpty();
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<ul class=\"config-list\">\n");
        for (ConfigKey key : configs) {
            if (key.internalConfig) {
                continue;
            }
            b.append("<li>\n");
            b.append(String.format("<h%1$d>" +
                    "<a id=\"%3$s\"></a><a id=\"%2$s\" href=\"#%2$s\">%3$s</a>" +
                    "</h%1$d>%n", headerDepth, idGenerator.apply(key.name), key.name));
            b.append("<p>");
            if (key.documentation != null) {
                b.append(key.documentation.replaceAll("\n", "<br>"));
            }
            b.append("</p>\n");

            b.append("<table>" +
                    "<tbody>\n");
            for (String detail : headers()) {
                if (detail.equals("Name") || detail.equals("Description"))
                    continue;
                if (detail.equals("Default") && key.alternativeString != null) {
                    addConfigDetail(b, detail, key.alternativeString);
                    continue;
                }
                addConfigDetail(b, detail, getConfigValue(key, detail));
            }
            if (hasUpdateModes) {
                String updateMode = dynamicUpdateModes.get(key.name);
                if (updateMode == null)
                    updateMode = "read-only";
                addConfigDetail(b, "Update Mode", updateMode);
            }
            b.append("</tbody></table>\n");
            b.append("</li>\n");
        }
        b.append("</ul>\n");
        return b.toString();
    }
    
    private static void addConfigDetail(StringBuilder builder, String name, String value) {
        builder.append("<tr>" +
                "<th>" + name + ":</th>" +
                "<td>" + value + "</td>" +
                "</tr>\n");
    }
}
