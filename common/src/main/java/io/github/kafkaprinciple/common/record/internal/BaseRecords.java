package io.github.kafkaprinciple.common.record.internal;

public interface BaseRecords {
    int sizeInBytes();

    RecordsSend<? extends BaseRecords> toSend();
}
