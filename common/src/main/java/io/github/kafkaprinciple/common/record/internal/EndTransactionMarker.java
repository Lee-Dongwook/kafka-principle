package io.github.kafkaprinciple.common.record.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.kafkaprinciple.common.errors.InvalidRecordException;
import io.github.kafkaprinciple.common.message.EndTxnMarker;
import io.github.kafkaprinciple.common.protocol.ByteBufferAccessor;
import io.github.kafkaprinciple.common.protocol.MessageUtil;

import java.nio.ByteBuffer;

public class EndTransactionMarker {
    private static final Logger log = LoggerFactory.getLogger(EndTransactionMarker.class);

    private final ControlRecordType type;
    private final int coordinatorEpoch;
    private final ByteBuffer buffer;

    public EndTransactionMarker(ControlRecordType type, int coordinatorEpoch) {
        ensureTransactionMarkerControlType(type);
        this.type = type;
        this.coordinatorEpoch = coordinatorEpoch;
        EndTxnMarker marker = new EndTxnMarker().setCoordinatorEpoch(coordinatorEpoch);
        this.buffer = MessageUtil.toVersionPrefixedByteBuffer(EndTxnMarker.HIGHEST_SUPPORTED_VERSION, marker);
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public ControlRecordType controlType() {
        return type;
    }

    public ByteBuffer serializeValue() {
        return buffer.duplicate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        EndTransactionMarker that = (EndTransactionMarker) o;
        return coordinatorEpoch == that.coordinatorEpoch && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + coordinatorEpoch;
        return result;
    }

    private static void ensureTransactionMarkerControlType(ControlRecordType type) {
        if (type != ControlRecordType.COMMIT && type != ControlRecordType.ABORT)
            throw new IllegalArgumentException("Invalid control record type for end transaction marker " + type);
    }

    public static EndTransactionMarker deserialize(Record record) {
        ControlRecordType type = ControlRecordType.parse(record.key());
        return deserializeValue(type, record.value());
    }

    static EndTransactionMarker deserializeValue(ControlRecordType type, ByteBuffer value) {
        ensureTransactionMarkerControlType(type);

        short version = value.getShort();
        if (version < EndTxnMarker.LOWEST_SUPPORTED_VERSION)
            throw new InvalidRecordException("Invalid version found for end transaction marker: " + version +
                    ". May indicate data corruption");

        if (version > EndTxnMarker.HIGHEST_SUPPORTED_VERSION) {
            log.debug("Received end transaction marker value version {}. Parsing as version {}", version,
                    EndTxnMarker.HIGHEST_SUPPORTED_VERSION);
            version = EndTxnMarker.HIGHEST_SUPPORTED_VERSION;
        }
        EndTxnMarker marker = new EndTxnMarker(new ByteBufferAccessor(value), version);
        return new EndTransactionMarker(type, marker.coordinatorEpoch());
    }

    public int endTxnMarkerValueSize() {
        return DefaultRecord.sizeInBytes(0, 0L,
                type.controlRecordKeySize(),
                buffer.remaining(),
                Record.EMPTY_HEADERS);
    }
}
