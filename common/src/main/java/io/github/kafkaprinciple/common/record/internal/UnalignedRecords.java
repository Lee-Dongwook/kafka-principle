package io.github.kafkaprinciple.common.record.internal;

public interface UnalignedRecords extends TransferableRecords {
    @Override
    default RecordsSend<? extends BaseRecords> toSend() {
        return new DefaultRecordsSend<>(this, sizeInBytes());
    }
}
