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


public class EndTxnMarker implements ApiMessage {
    int coordinatorEpoch;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("coordinator_epoch", Type.INT32, "The coordinator epoch when appending the record")
        );

    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;

    public EndTxnMarker(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public EndTxnMarker() {
        this.coordinatorEpoch = 0;
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
        this.coordinatorEpoch = _readable.readInt();
        this._unknownTaggedFields = null;
    }

    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(coordinatorEpoch);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }

    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
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
        if (!(obj instanceof EndTxnMarker)) return false;
        EndTxnMarker other = (EndTxnMarker) obj;
        if (coordinatorEpoch != other.coordinatorEpoch) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + coordinatorEpoch;
        return hashCode;
    }

    @Override
    public EndTxnMarker duplicate() {
        EndTxnMarker _duplicate = new EndTxnMarker();
        _duplicate.coordinatorEpoch = coordinatorEpoch;
        return _duplicate;
    }

    @Override
    public String toString() {
        return "EndTxnMarker("
            + "coordinatorEpoch=" + coordinatorEpoch
            + ")";
    }

    public int coordinatorEpoch() {
        return this.coordinatorEpoch;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public EndTxnMarker setCoordinatorEpoch(int v) {
        this.coordinatorEpoch = v;
        return this;
    }
}
