package io.github.kafkaprinciple.common.record;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.NoSuchElementException;

@InterfaceAudience.Public
public enum TimestampType {
    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"), CREATE_TIME(0, "CreateTime"), LOG_APPEND_TIME(1, "LogAppendTime");

    public final int id;
    public final String name;

    TimestampType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static TimestampType forName(String name) {
        for (TimestampType t : values())
            if (t.name.equals(name))
                return t;
        throw new NoSuchElementException("Invalid timestamp type " + name);
    }
    
    @Override
    public String toString() {
        return name;
    }
}
