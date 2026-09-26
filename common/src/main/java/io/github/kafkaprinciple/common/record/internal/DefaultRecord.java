package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.header.Header;
import io.github.kafkaprinciple.common.header.internals.RecordHeader;
import io.github.kafkaprinciple.common.record.TimestampType;
import io.github.kafkaprinciple.common.utils.Utils;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import static io.github.kafkaprinciple.common.record.internal.RecordBatch.MAGIC_VALUE_V2;

public class DefaultRecord implements Record {
    public static final int MAX_RECORD_OVERHEAD = 21;
    private static final int NULL_VARINT_SIZE_BYTES = ByteUtils.sizeOfVarint(-1);

    private final int sizeInBytes;
    private final byte attributes;
    private final long offset;
    private final long timestamp;
    private final int sequence;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Header[] headers;

     DefaultRecord(int sizeInBytes,
                  byte attributes,
                  long offset,
                  long timestamp,
                  int sequence,
                  ByteBuffer key,
                  ByteBuffer value,
             Header[] headers) {
         this.sizeInBytes = sizeInBytes;
         this.attributes = attributes;
         this.offset = offset;
         this.timestamp = timestamp;
         this.sequence = sequence;
         this.key = key;
         this.value = value;
         this.headers = headers;
     }
    
    @Override
    public long offset() {
        return offset;
    }

    @Override
    public int sequence() {
        return sequence;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public byte attributes() {
        return attributes;
    }

    @Override
    public void ensureValid() {}

    @Override
    public int keySize() {
        return key == null ? -1 : key.remaining();
    }

    @Override
    public int valueSize() {
        return value == null ? -1 : value.remaining();
    }

    @Override
    public boolean hasKey() {
        return key != null;
    }

    @Override
    public ByteBuffer key() {
        return key == null ? null : key.duplicate();
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    @Override
    public Header[] headers() {
        return headers;
    }

    public static int writeTo(DataOutputStream out,
                              int offsetDelta,
                              long timestampDelta,
                              ByteBuffer key,
                              ByteBuffer value,
            Header[] headers) throws IOException {
        int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        ByteUtils.writeVarint(sizeInBytes, out);

        byte attributes = 0;
        out.write(attributes);

        ByteUtils.writeVarlong(timestampDelta, out);
        ByteUtils.writeVarint(offsetDelta, out);

        if (key == null) {
            ByteUtils.writeVarint(-1, out);
        } else {
            int keySize = key.remaining();
            ByteUtils.writeVarint(keySize, out);
            Utils.writeTo(out, key, keySize);
        }

        if (value == null) {
            ByteUtils.writeVarint(-1, out);
        } else {
            int valueSize = value.remaining();
            ByteUtils.writeVarint(valueSize, out);
            Utils.writeTo(out, value, valueSize);
        }

        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        ByteUtils.writeVarint(headers.length, out);

        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            byte[] utf8Bytes = Utils.utf8(headerKey);
            ByteUtils.writeVarint(utf8Bytes.length, out);
            out.write(utf8Bytes);

            byte[] headerValue = header.value();
            if (headerValue == null) {
                ByteUtils.writeVarint(-1, out);
            } else {
                ByteUtils.writeVarint(headerValue.length, out);
                out.write(headerValue);
            }
        }

        return ByteUtils.sizeOfVarint(sizeInBytes) + sizeInBytes;
    }
    
    @Override
    public boolean hasMagic(byte magic) {
        return magic >= MAGIC_VALUE_V2;
    }

    @Override
    public boolean isCompressed() {
        return false;
    }

    @Override
    public boolean hasTimestampType(TimestampType timestampType) {
        return false;
    }

    @Override
    public String toString() {
        return String.format("DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
                offset,
                timestamp,
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DefaultRecord that = (DefaultRecord) o;
        return sizeInBytes == that.sizeInBytes &&
                attributes == that.attributes &&
                offset == that.offset &&
                timestamp == that.timestamp &&
                sequence == that.sequence &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = sizeInBytes;
        result = 31 * result + (int) attributes;
        result = 31 * result + Long.hashCode(offset);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + sequence;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    public static DefaultRecord readFrom(InputStream input,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
                                         Long logAppendTime,
            int maxRecordBodySize) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        if (sizeOfBodyInBytes < 0)
            throw new InvalidRecordException("Invalid record size: " + sizeOfBodyInBytes + " is negative.");
        if (sizeOfBodyInBytes > maxRecordBodySize)
            throw new InvalidRecordException("Invalid record size: " + sizeOfBodyInBytes +
                    " exceeds the configured maximum record size of " + maxRecordBodySize + ".");
        ByteBuffer recordBuffer = ByteBuffer.allocate(sizeOfBodyInBytes);
        int bytesRead = Utils.readFully(input, recordBuffer);
        if (bytesRead != sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                    " bytes in record payload, but the record payload reached EOF.");
        recordBuffer.flip();
        return readFrom(recordBuffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }
    
    public static DefaultRecord readFrom(ByteBuffer buffer,
                                         long baseOffset,
                                         long baseTimestamp,
                                         int baseSequence,
            Long logAppendTime) {
        int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        return readFrom(buffer, sizeOfBodyInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }
    
    private static DefaultRecord readFrom(ByteBuffer buffer,
                                          int sizeOfBodyInBytes,
                                          long baseOffset,
                                          long baseTimestamp,
                                          int baseSequence,
            Long logAppendTime) {
        if (buffer.remaining() < sizeOfBodyInBytes)
            throw new InvalidRecordException("Invalid record size: expected " + sizeOfBodyInBytes +
                    " bytes in record payload, but instead the buffer has only " + buffer.remaining() +
                    " remaining bytes.");
        try {
            int recordStart = buffer.position();
            byte attributes = buffer.get();
            long timestampDelta = ByteUtils.readVarlong(buffer);
            long timestamp = baseTimestamp + timestampDelta;
            if (logAppendTime != null)
                timestamp = logAppendTime;

            int offsetDelta = ByteUtils.readVarint(buffer);
            long offset = baseOffset + offsetDelta;
            int sequence = baseSequence >= 0 ? DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta)
                    : RecordBatch.NO_SEQUENCE;

            // read key
            int keySize = ByteUtils.readVarint(buffer);
            ByteBuffer key = Utils.readBytes(buffer, keySize);

            // read value
            int valueSize = ByteUtils.readVarint(buffer);
            ByteBuffer value = Utils.readBytes(buffer, valueSize);

            int numHeaders = ByteUtils.readVarint(buffer);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            if (numHeaders > buffer.remaining())
                throw new InvalidRecordException("Found invalid number of record headers. " + numHeaders
                        + " is larger than the remaining size of the buffer");

            final Header[] headers;
            if (numHeaders == 0)
                headers = Record.EMPTY_HEADERS;
            else
                headers = readHeaders(buffer, numHeaders);

            if (buffer.position() - recordStart != sizeOfBodyInBytes)
                throw new InvalidRecordException("Invalid record size: expected to read " + sizeOfBodyInBytes +
                        " bytes in record payload, but instead read " + (buffer.position() - recordStart));

            int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
            return new DefaultRecord(totalSizeInBytes, attributes, offset, timestamp, sequence, key, value, headers);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }
    
    public static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                         long baseOffset,
                                                         long baseTimestamp,
                                                         int baseSequence,
                                                         Long logAppendTime,
            int maxRecordBodySize) throws IOException {
        int sizeOfBodyInBytes = ByteUtils.readVarint(input);
        if (sizeOfBodyInBytes < 0)
            throw new InvalidRecordException("Invalid record size: " + sizeOfBodyInBytes + " is negative.");
        if (sizeOfBodyInBytes > maxRecordBodySize)
            throw new InvalidRecordException("Invalid record size: " + sizeOfBodyInBytes +
                    " exceeds the configured maximum record size of " + maxRecordBodySize + ".");
        int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;

        return readPartiallyFrom(input, totalSizeInBytes, baseOffset, baseTimestamp,
                baseSequence, logAppendTime);
    }
    
    private static PartialDefaultRecord readPartiallyFrom(InputStream input,
                                                          int sizeInBytes,
                                                          long baseOffset,
                                                          long baseTimestamp,
                                                          int baseSequence,
                                                          Long logAppendTime) throws IOException {
        try {
            byte attributes = (byte) input.read();
            long timestampDelta = ByteUtils.readVarlong(input);
            long timestamp = baseTimestamp + timestampDelta;
            if (logAppendTime != null)
                timestamp = logAppendTime;

            int offsetDelta = ByteUtils.readVarint(input);
            long offset = baseOffset + offsetDelta;
            int sequence = baseSequence >= 0 ?
                DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
                RecordBatch.NO_SEQUENCE;

            int keySize = ByteUtils.readVarint(input);
            skipBytes(input, keySize);

            int valueSize = ByteUtils.readVarint(input);
            skipBytes(input, valueSize);

            
            int numHeaders = ByteUtils.readVarint(input);
            if (numHeaders < 0)
                throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
            for (int i = 0; i < numHeaders; i++) {
                int headerKeySize = ByteUtils.readVarint(input);
                if (headerKeySize < 0)
                    throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);
                skipBytes(input, headerKeySize);

                int headerValueSize = ByteUtils.readVarint(input);
                skipBytes(input, headerValueSize);
            }

            return new PartialDefaultRecord(sizeInBytes, attributes, offset, timestamp, sequence, keySize, valueSize);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new InvalidRecordException("Found invalid record structure", e);
        }
    }

    private static void skipBytes(InputStream in, int bytesToSkip) throws IOException {
        if (bytesToSkip <= 0)
            return;

        while (bytesToSkip > 0) {
            int ns = (int) in.skip(bytesToSkip);
            if (ns > 0 && ns <= bytesToSkip) {
                bytesToSkip -= ns;
            } else if (ns == 0) {
                if (in.read() == -1) {
                    throw new InvalidRecordException("Reached end of input stream before skipping all bytes. " +
                            "Remaining bytes:" + bytesToSkip);
                }
                bytesToSkip--;
            } else {
                throw new IOException("Unable to skip exactly");
            }
        }
    }
    
    private static Header[] readHeaders(ByteBuffer buffer, int numHeaders) {
        Header[] headers = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            int headerKeySize = ByteUtils.readVarint(buffer);
            if (headerKeySize < 0)
                throw new InvalidRecordException("Invalid negative header key size " + headerKeySize);

            ByteBuffer headerKeyBuffer = Utils.readBytes(buffer, headerKeySize);

            int headerValueSize = ByteUtils.readVarint(buffer);
            ByteBuffer headerValue = Utils.readBytes(buffer, headerValueSize);

            headers[i] = new RecordHeader(headerKeyBuffer, headerValue);
        }

        return headers;
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  ByteBuffer key,
                                  ByteBuffer value,
                                  Header[] headers) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    public static int sizeInBytes(int offsetDelta,
                                  long timestampDelta,
                                  int keySize,
                                  int valueSize,
                                  Header[] headers) {
        int bodySize = sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBodyInBytes(int offsetDelta,
                                         long timestampDelta,
                                         ByteBuffer key,
                                         ByteBuffer value,
            Header[] headers) {
        int keySize = key == null ? -1 : key.remaining();
        int valueSize = value == null ? -1 : value.remaining();
        return sizeOfBodyInBytes(offsetDelta, timestampDelta, keySize, valueSize, headers);
    }
    
    public static int sizeOfBodyInBytes(int offsetDelta,
                                        long timestampDelta,
                                        int keySize,
                                        int valueSize,
            Header[] headers) {
        int size = 1; 
        size += ByteUtils.sizeOfVarint(offsetDelta);
        size += ByteUtils.sizeOfVarlong(timestampDelta);
        size += sizeOf(keySize, valueSize, headers);
        return size;
    }
    
    private static int sizeOf(int keySize, int valueSize, Header[] headers) {
        int size = 0;
        if (keySize < 0)
            size += NULL_VARINT_SIZE_BYTES;
        else
            size += ByteUtils.sizeOfVarint(keySize) + keySize;

        if (valueSize < 0)
            size += NULL_VARINT_SIZE_BYTES;
        else
            size += ByteUtils.sizeOfVarint(valueSize) + valueSize;

        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        size += ByteUtils.sizeOfVarint(headers.length);
        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            int headerKeySize = Utils.utf8Length(headerKey);
            size += ByteUtils.sizeOfVarint(headerKeySize) + headerKeySize;

            byte[] headerValue = header.value();
            if (headerValue == null) {
                size += NULL_VARINT_SIZE_BYTES;
            } else {
                size += ByteUtils.sizeOfVarint(headerValue.length) + headerValue.length;
            }
        }
        return size;
    }

    static int recordSizeUpperBound(ByteBuffer key, ByteBuffer value, Header[] headers) {
        int keySize = key == null ? -1 : key.remaining();
        int valueSize = value == null ? -1 : value.remaining();
        return MAX_RECORD_OVERHEAD + sizeOf(keySize, valueSize, headers);
    }
}
