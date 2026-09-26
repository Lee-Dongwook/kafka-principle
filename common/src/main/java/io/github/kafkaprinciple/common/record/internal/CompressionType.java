package io.github.kafkaprinciple.common.record.internal;

import java.util.zip.Deflater;

public enum CompressionType {
    NONE((byte) 0, "none", 1.0f),
            
    GZIP((byte) 1, "gzip", 1.0f) {
        public static final int MIN_LEVEL = Deflater.BEST_SPEED;
        public static final int MAX_LEVEL = Deflater.BEST_COMPRESSION;
        public static final int DEFAULT_LEVEL = Deflater.DEFAULT_COMPRESSION;

        @Override
        public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        public ConfigDef.Validator levelValidator() {
            return ConfigDef.LambdaValidator.with((name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int level = ((Number) value).intValue();
                if (level > MAX_LEVEL || (level < MIN_LEVEL && level != DEFAULT_LEVEL)) {
                    throw new ConfigException(name, value, "Value must be between " + MIN_LEVEL + " and " + MAX_LEVEL
                            + " or equal to " + DEFAULT_LEVEL);
                }
            }, () -> "[" + MIN_LEVEL + ",...," + MAX_LEVEL + "] or " + DEFAULT_LEVEL);
        }
    }
    
    public final byte id;
    public final String name;
    public final float rate;

    CompressionType(byte id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    public static CompressionType forId(int id) {

    }

    public static CompressionType forName(String name) {

    }

    public int defaultLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public int maxLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public int minLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator levelValidator() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
