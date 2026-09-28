package io.github.kafkaprinciple.common.quota;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Collection;
import java.util.Objects;

@InterfaceAudience.Public
public class ClientQuotaAlteration {
    public static class Op {
        private final String key;
        private final Double value;

        public Op(String key, Double value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return this.key;
        }

        public Double value() {
            return this.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Op that = (Op) o;
            return Objects.equals(key, that.key) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "ClientQuotaAlteration.Op(key=" + key + ", value=" + value + ")";
        }
    }

    private final ClientQuotaEntity entity;
    private final Collection<Op> ops;

    public ClientQuotaAlteration(ClientQuotaEntity entity, Collection<Op> ops) {
        this.entity = entity;
        this.ops = ops;
    }

    public ClientQuotaEntity entity() {
        return this.entity;
    }

    public Collection<Op> ops() {
        return this.ops;
    }

    @Override
    public String toString() {
        return "ClientQuotaAlteration(entity=" + entity + ", ops=" + ops + ")";
    }
}
