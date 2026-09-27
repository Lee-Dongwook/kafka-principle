package io.github.kafkaprinciple.common.message;

import io.github.kafkaprinciple.common.protocol.ApiMessage;
import io.github.kafkaprinciple.common.protocol.MessageSizeAccumulator;
import io.github.kafkaprinciple.common.protocol.MessageUtil;
import io.github.kafkaprinciple.common.protocol.ObjectSerializationCache;
import io.github.kafkaprinciple.common.protocol.Readable;
import io.github.kafkaprinciple.common.protocol.Writable;
import io.github.kafkaprinciple.common.protocol.types.Field;
import io.github.kafkaprinciple.common.protocol.types.RawTaggedField;
import io.github.kafkaprinciple.common.protocol.types.RawTaggedFieldWriter;
import io.github.kafkaprinciple.common.protocol.types.Schema;
import io.github.kafkaprinciple.common.protocol.types.Type;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;
import java.util.ArrayList;
import java.util.List;

import static io.github.kafkaprinciple.common.protocol.types.Field.TaggedFieldsSection;


public class KRaftVersionRecord implements ApiMessage {
    short version;
    short kRaftVersion;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("version", Type.INT16, "The version of the kraft version record."),
            new Field("kraft_version", Type.INT16, "The kraft protocol version."),
            TaggedFieldsSection.of(
            )
        );

    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;

    public KRaftVersionRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public KRaftVersionRecord() {
        this.version = (short) 0;
        this.kRaftVersion = (short) 0;
    }

    @Override
    public short apiKey() {
        return -1;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 0;
    }

    @Override
    public final void read(Readable _readable, short _version) {
        this.version = _readable.readShort();
        this.kRaftVersion = _readable.readShort();
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        if (_numTaggedFields < 0) {
            throw new RuntimeException("Invalid negative number of tagged fields " + _numTaggedFields);
        }
        if (_numTaggedFields > _readable.remaining()) {
            throw new RuntimeException("Tried to read " + _numTaggedFields + " tagged fields, but there are only " + _readable.remaining() + " bytes remaining.");
        }
        if (_numTaggedFields > MessageUtil.MAX_TAGGED_FIELD_COUNT) {
            throw new RuntimeException("Tried to read " + _numTaggedFields + " tagged fields, which exceeds the maximum allowed count of " + MessageUtil.MAX_TAGGED_FIELD_COUNT + ".");
        }
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                    break;
            }
        }
    }

    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeShort(version);
        _writable.writeShort(kRaftVersion);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }

    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(2);
        _size.addBytes(2);
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KRaftVersionRecord)) return false;
        KRaftVersionRecord other = (KRaftVersionRecord) obj;
        if (version != other.version) return false;
        if (kRaftVersion != other.kRaftVersion) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + version;
        hashCode = 31 * hashCode + kRaftVersion;
        return hashCode;
    }

    @Override
    public KRaftVersionRecord duplicate() {
        KRaftVersionRecord _duplicate = new KRaftVersionRecord();
        _duplicate.version = version;
        _duplicate.kRaftVersion = kRaftVersion;
        return _duplicate;
    }

    @Override
    public String toString() {
        return "KRaftVersionRecord("
            + "version=" + version
            + ", kRaftVersion=" + kRaftVersion
            + ")";
    }

    public short version() {
        return this.version;
    }

    public short kRaftVersion() {
        return this.kRaftVersion;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public KRaftVersionRecord setVersion(short v) {
        this.version = v;
        return this;
    }

    public KRaftVersionRecord setKRaftVersion(short v) {
        this.kRaftVersion = v;
        return this;
    }
}
