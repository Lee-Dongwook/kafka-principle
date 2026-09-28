package io.github.kafkaprinciple.common.message;

import io.github.kafkaprinciple.common.errors.UnsupportedVersionException;
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


public class ControlRecordTypeSchema implements ApiMessage {
    short type;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("type", Type.INT16, "The type of the control record, such as commit or abort")
        );

    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;

    public ControlRecordTypeSchema(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public ControlRecordTypeSchema() {
        this.type = (short) 0;
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
        this.type = _readable.readShort();
        this._unknownTaggedFields = null;
    }

    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeShort(type);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }

    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(2);
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ControlRecordTypeSchema)) return false;
        ControlRecordTypeSchema other = (ControlRecordTypeSchema) obj;
        if (type != other.type) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + type;
        return hashCode;
    }

    @Override
    public ControlRecordTypeSchema duplicate() {
        ControlRecordTypeSchema _duplicate = new ControlRecordTypeSchema();
        _duplicate.type = type;
        return _duplicate;
    }

    @Override
    public String toString() {
        return "ControlRecordTypeSchema("
            + "type=" + type
            + ")";
    }

    public short type() {
        return this.type;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public ControlRecordTypeSchema setType(short v) {
        this.type = v;
        return this;
    }
}
