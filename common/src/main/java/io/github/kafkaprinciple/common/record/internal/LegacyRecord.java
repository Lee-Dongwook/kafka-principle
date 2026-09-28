package io.github.kafkaprinciple.common.record.internal;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import io.github.kafkaprinciple.common.errors.CorruptRecordException;
import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.record.TimestampType;
import io.github.kafkaprinciple.common.utils.Utils;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;
import io.github.kafkaprinciple.common.utils.internals.Checksums;
import io.github.kafkaprinciple.common.utils.internals.SingleByteBufferOutputStream;

import static io.github.kafkaprinciple.common.utils.Utils.wrapNullable;

public final class LegacyRecord {
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTES_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;

    public static final int HEADER_SIZE_V0 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH;
    public static final int HEADER_SIZE_V1 = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH;

    public static final int RECORD_OVERHEAD_V0 = HEADER_SIZE_V0 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
    public static final int RECORD_OVERHEAD_V1 = HEADER_SIZE_V1 + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    private static final byte COMPRESSION_CODEC_MASK = 0x07;
    private static final byte TIMESTAMP_TYPE_MASK = 0x08;

    public static final long NO_TIMESTAMP = -1L;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public LegacyRecord(ByteBuffer buffer) {
        this(buffer, null, null);
    }

    public LegacyRecord(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    public long computeChecksum() {
        return crc32(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    public boolean isValid() {
        return sizeInBytes() >= RECORD_OVERHEAD_V0 && checksum() == computeChecksum();
    }

    public void ensureValid() {
        if (sizeInBytes() < RECORD_OVERHEAD_V0)
            throw new CorruptRecordException("Record is corrupt (crc could not be retrieved as the record is too "
                    + "small, size = " + sizeInBytes() + ")");

        if (!isValid())
            throw new CorruptRecordException("Record is corrupt (stored crc = " + checksum()
                    + ", computed crc = " + computeChecksum() + ")");
    }

    public int sizeInBytes() {
        return buffer.limit();
    }

    public int keySize() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        else
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
    }

    public boolean hasKey() {
        return keySize() >= 0;
    }

    private int valueSizeOffset() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        else
            return KEY_OFFSET_V1 + Math.max(0, keySize());
    }

    public int valueSize() {
        return buffer.getInt(valueSizeOffset());
    }

    public boolean hasNullValue() {
        return valueSize() < 0;
    }

    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    public long timestamp() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return RecordBatch.NO_TIMESTAMP;
        else {
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null)
                return wrapperRecordTimestamp;
            else
                return buffer.getLong(TIMESTAMP_OFFSET);
        }
    }

    public TimestampType timestampType() {
        return timestampType(magic(), wrapperRecordTimestampType, attributes());
    }

    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    public ByteBuffer value() {
        return Utils.sizeDelimited(buffer, valueSizeOffset());
    }

    public ByteBuffer key() {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V0);
        else
            return Utils.sizeDelimited(buffer, KEY_SIZE_OFFSET_V1);
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    public String toString() {
        if (magic() > 0)
            return String.format(
                    "Record(magic=%d, attributes=%d, compression=%s, crc=%d, %s=%d, key=%d bytes, value=%d bytes)",
                    magic(),
                    attributes(),
                    compressionType(),
                    checksum(),
                    timestampType(),
                    timestamp(),
                    key() == null ? 0 : key().limit(),
                    value() == null ? 0 : value().limit());
        else
            return String.format(
                    "Record(magic=%d, attributes=%d, compression=%s, crc=%d, key=%d bytes, value=%d bytes)",
                    magic(),
                    attributes(),
                    compressionType(),
                    checksum(),
                    key() == null ? 0 : key().limit(),
                    value() == null ? 0 : value().limit());
    }

    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (!other.getClass().equals(LegacyRecord.class))
            return false;
        LegacyRecord record = (LegacyRecord) other;
        return this.buffer.equals(record.buffer);
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    public static LegacyRecord create(byte magic,
            long timestamp,
            byte[] key,
            byte[] value,
            CompressionType compressionType,
            TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new LegacyRecord(buffer);
    }

    public static LegacyRecord create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static void writeCompressedRecordHeader(ByteBuffer buffer,
            byte magic,
            int recordSize,
            long timestamp,
            CompressionType compressionType,
            TimestampType timestampType) {
        int recordPosition = buffer.position();
        int valueSize = recordSize - recordOverhead(magic);

        write(buffer, magic, timestamp, null, null, compressionType, timestampType);
        buffer.position(recordPosition);

        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        long crc = crc32(buffer, MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    private static void write(ByteBuffer buffer,
            byte magic,
            long timestamp,
            ByteBuffer key,
            ByteBuffer value,
            CompressionType compressionType,
            TimestampType timestampType) {
        try (DataOutputStream out = new DataOutputStream(new SingleByteBufferOutputStream(buffer))) {
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static long write(DataOutputStream out,
            byte magic,
            long timestamp,
            byte[] key,
            byte[] value,
            CompressionType compressionType,
            TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    public static long write(DataOutputStream out,
            byte magic,
            long timestamp,
            ByteBuffer key,
            ByteBuffer value,
            CompressionType compressionType,
            TimestampType timestampType) throws IOException {
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }

    public static void write(DataOutputStream out,
            byte magic,
            long crc,
            byte attributes,
            long timestamp,
            byte[] key,
            byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    private static void write(DataOutputStream out,
            byte magic,
            long crc,
            byte attributes,
            long timestamp,
            ByteBuffer key,
            ByteBuffer value) throws IOException {
        if (magic != RecordBatch.MAGIC_VALUE_V0 && magic != RecordBatch.MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        out.writeInt((int) (crc & 0xffffffffL));

        out.writeByte(magic);

        out.writeByte(attributes);

        if (magic > RecordBatch.MAGIC_VALUE_V0)
            out.writeLong(timestamp);

        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            Utils.writeTo(out, key, size);
        }

        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            Utils.writeTo(out, value, size);
        }
    }

    static int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
        return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
    }

    public static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0)
            attributes |= (byte) (COMPRESSION_CODEC_MASK & type.id);
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE)
                throw new IllegalArgumentException("Timestamp type must be provided to compute attributes for " +
                        "message format v1");
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                attributes |= TIMESTAMP_TYPE_MASK;
        }
        return attributes;
    }

    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    private static long crc32(ByteBuffer buffer, int offset, int size) {
        CRC32 crc = new CRC32();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }

    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        CRC32 crc = new CRC32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            Checksums.updateLong(crc, timestamp);

        if (key == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = key.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, key, size);
        }

        if (value == null) {
            Checksums.updateInt(crc, -1);
        } else {
            int size = value.remaining();
            Checksums.updateInt(crc, size);
            Checksums.update(crc, value, size);
        }
        return crc.getValue();
    }

    static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        else if (magic == 1)
            return RECORD_OVERHEAD_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    static int headerSize(byte magic) {
        if (magic == 0)
            return HEADER_SIZE_V0;
        else if (magic == 1)
            return HEADER_SIZE_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        else if (magic == 1)
            return KEY_OFFSET_V1;
        throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
    }

    public static TimestampType timestampType(byte magic, TimestampType wrapperRecordTimestampType, byte attributes) {
        if (magic == 0)
            return TimestampType.NO_TIMESTAMP_TYPE;
        else if (wrapperRecordTimestampType != null)
            return wrapperRecordTimestampType;
        else
            return (attributes & TIMESTAMP_TYPE_MASK) == 0 ? TimestampType.CREATE_TIME : TimestampType.LOG_APPEND_TIME;
    }
}
