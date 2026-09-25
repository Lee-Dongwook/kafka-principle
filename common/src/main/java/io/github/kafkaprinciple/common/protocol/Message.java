package io.github.kafkaprinciple.common.protocol;

import io.github.kafkaprinciple.common.protocol.types.RawTaggedField;

import java.util.List;

public interface Message {
    short lowestSupportedVersion();

    short highestSupportedVersion();

    default int size(ObjectSerializationCache cache, short version) {
        MessageSizeAccumulator size = new MessageSizeAccumulator();
        addSize(size, cache, version);
        return size.totalSize();
    }

    void addSize(MessageSizeAccumulator size, ObjectSerializationCache cache, short version);

    void write(Writable writable, ObjectSerializationCache cache, short version);

    void read(Readable readable, short version);

    List<RawTaggedField> unknownTaggedFields();

    Message duplicate();
}
