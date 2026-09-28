package io.github.kafkaprinciple.common.record.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.kafkaprinciple.common.errors.InvalidRecordException;
import io.github.kafkaprinciple.common.message.ControlRecordTypeSchema;
import io.github.kafkaprinciple.common.protocol.ByteBufferAccessor;
import io.github.kafkaprinciple.common.protocol.MessageUtil;

import java.nio.ByteBuffer;

public enum ControlRecordType {
    ABORT((short) 0),
    COMMIT((short) 1),
    LEADER_CHANGE((short) 2),
    SNAPSHOT_HEADER((short) 3),
    SNAPSHOT_FOOTER((short) 4),
    KRAFT_VERSION((short) 5),
    KRAFT_VOTERS((short) 6),
    UNKNOWN((short) -1);

    private static final Logger log = LoggerFactory.getLogger(ControlRecordType.class);
    private static final int CONTROL_RECORD_KEY_SIZE = 4;

    private final short type;
    private final ByteBuffer buffer;

    ControlRecordType(short type) {
        this.type = type;
        ControlRecordTypeSchema schema = new ControlRecordTypeSchema().setType(type);
        buffer = MessageUtil.toVersionPrefixedByteBuffer(ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION, schema);
    }

    public short type() {
        return type;
    }

    public ByteBuffer recordKey() {
        if (this == UNKNOWN)
            throw new IllegalArgumentException("Cannot serialize UNKNOWN control record type");
        return buffer.duplicate();
    }

    public int controlRecordKeySize() {
        return buffer.remaining();
    }

    public static short parseTypeId(ByteBuffer key) {
        ByteBuffer buffer = key.duplicate();

        if (buffer.remaining() < CONTROL_RECORD_KEY_SIZE)
            throw new InvalidRecordException("Invalid value size found for control record key. " +
                    "Must have at least " + CONTROL_RECORD_KEY_SIZE + " bytes, but found only " + buffer.remaining());

        short version = buffer.getShort();
        if (version < ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION)
            throw new InvalidRecordException("Invalid version found for control record: " + version +
                    ". May indicate data corruption");

        if (version > ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION) {
            log.debug("Received unknown control record key version {}. Parsing as version {}", version,
                    ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION);
            version = ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION;
        }
        ControlRecordTypeSchema schema = new ControlRecordTypeSchema(new ByteBufferAccessor(buffer), version);
        return schema.type();
    }

    public static ControlRecordType fromTypeId(short typeId) {
        switch (typeId) {
            case 0:
                return ABORT;
            case 1:
                return COMMIT;
            case 2:
                return LEADER_CHANGE;
            case 3:
                return SNAPSHOT_HEADER;
            case 4:
                return SNAPSHOT_FOOTER;
            case 5:
                return KRAFT_VERSION;
            case 6:
                return KRAFT_VOTERS;

            default:
                return UNKNOWN;
        }
    }

    public static ControlRecordType parse(ByteBuffer key) {
        return fromTypeId(parseTypeId(key));
    }
}
