package io.github.kafkaprinciple.common.network;

import java.util.Locale;
import java.util.Objects;

import io.github.kafkaprinciple.common.config.ConfigException;
import io.github.kafkaprinciple.common.security.auth.SecurityProtocol;
import io.github.kafkaprinciple.common.utils.Utils;

public final class ListenerName {
    private static final String CONFIG_STATIC_PREFIX = "listener.name";

    public static ListenerName forSecurityProtocol(SecurityProtocol securityProtocol) {
        return new ListenerName(securityProtocol.name);
    }

    public static ListenerName normalised(String value) {
        if (Utils.isBlank(value)) {
            throw new ConfigException("The provided listener name is null or empty string");
        }
        return new ListenerName(value.toUpperCase(Locale.ROOT));
    }

    private final String value;

    public ListenerName(String value) {
        Objects.requireNonNull(value, "value should not be null.");
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ListenerName))
            return false;
        ListenerName that = (ListenerName) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "ListenerName(" + value + ")";
    }

    public String configPrefix() {
        return CONFIG_STATIC_PREFIX + "." + value.toLowerCase(Locale.ROOT) + ".";
    }

    public String saslMechanismConfigPrefix(String saslMechanism) {
        return configPrefix() + saslMechanismPrefix(saslMechanism);
    }

    public static String saslMechanismPrefix(String saslMechanism) {
        return saslMechanism.toLowerCase(Locale.ROOT) + ".";
    }
}
