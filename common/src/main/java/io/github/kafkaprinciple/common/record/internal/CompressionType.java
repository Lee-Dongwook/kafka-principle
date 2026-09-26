package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.config.ConfigDef;
import io.github.kafkaprinciple.common.config.ConfigException;

import java.util.zip.Deflater;

import static io.github.kafkaprinciple.common.config.ConfigDef.Range.between;

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
    },
    SNAPPY((byte) 2, "snappy", 1.0f),
    LZ4((byte) 3, "lz4", 1.0f) {
        private static final int MIN_LEVEL = 1;
        private static final int MAX_LEVEL = 17;
        private static final int DEFAULT_LEVEL = 9;

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
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    },
    ZSTD((byte) 4, "zstd", 1.0f) {
        private static final int MIN_LEVEL = -131072;
        private static final int MAX_LEVEL = 22;
        private static final int DEFAULT_LEVEL = 3;

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
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    };
    
    public final byte id;
    public final String name;
    public final float rate;

    CompressionType(byte id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
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
