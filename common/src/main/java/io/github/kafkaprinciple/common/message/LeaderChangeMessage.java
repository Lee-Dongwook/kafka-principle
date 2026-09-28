package io.github.kafkaprinciple.common.message;

import io.github.kafkaprinciple.common.Uuid;
import io.github.kafkaprinciple.common.errors.UnsupportedVersionException;
import io.github.kafkaprinciple.common.protocol.ApiMessage;
import io.github.kafkaprinciple.common.protocol.Message;
import io.github.kafkaprinciple.common.protocol.MessageSizeAccumulator;
import io.github.kafkaprinciple.common.protocol.MessageUtil;
import io.github.kafkaprinciple.common.protocol.ObjectSerializationCache;
import io.github.kafkaprinciple.common.protocol.Readable;
import io.github.kafkaprinciple.common.protocol.Writable;
import io.github.kafkaprinciple.common.protocol.types.CompactArrayOf;
import io.github.kafkaprinciple.common.protocol.types.Field;
import io.github.kafkaprinciple.common.protocol.types.RawTaggedField;
import io.github.kafkaprinciple.common.protocol.types.RawTaggedFieldWriter;
import io.github.kafkaprinciple.common.protocol.types.Schema;
import io.github.kafkaprinciple.common.protocol.types.Type;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;
import java.util.ArrayList;
import java.util.List;

import static io.github.kafkaprinciple.common.protocol.types.Field.TaggedFieldsSection;


public class LeaderChangeMessage implements ApiMessage {
    short version;
    int leaderId;
    List<Voter> voters;
    List<Voter> grantingVoters;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("version", Type.INT16, "The version of the leader change message."),
            new Field("leader_id", Type.INT32, "The ID of the newly elected leader."),
            new Field("voters", new CompactArrayOf(Voter.SCHEMA_0), "The set of voters in the quorum for this epoch."),
            new Field("granting_voters", new CompactArrayOf(Voter.SCHEMA_0), "The voters who voted for the leader at the time of election."),
            TaggedFieldsSection.of(
            )
        );

    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("version", Type.INT16, "The version of the leader change message."),
            new Field("leader_id", Type.INT32, "The ID of the newly elected leader."),
            new Field("voters", new CompactArrayOf(Voter.SCHEMA_1), "The set of voters in the quorum for this epoch."),
            new Field("granting_voters", new CompactArrayOf(Voter.SCHEMA_1), "The voters who voted for the leader at the time of election."),
            TaggedFieldsSection.of(
            )
        );

    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1
    };

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 1;

    public LeaderChangeMessage(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public LeaderChangeMessage() {
        this.version = (short) 0;
        this.leaderId = 0;
        this.voters = new ArrayList<Voter>(0);
        this.grantingVoters = new ArrayList<Voter>(0);
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
        return 1;
    }

    @Override
    public final void read(Readable _readable, short _version) {
        this.version = _readable.readShort();
        this.leaderId = _readable.readInt();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field voters was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                if (arrayLength > MessageUtil.MAX_ARRAY_LENGTH) {
                    throw new RuntimeException("Tried to read a collection of size " + arrayLength + ", which exceeds the maximum allowed size of " + MessageUtil.MAX_ARRAY_LENGTH + ".");
                }
                ArrayList<Voter> newCollection = new ArrayList<>(Math.min(arrayLength, MessageUtil.MAX_PREALLOCATED_ARRAY_CAPACITY));
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Voter(_readable, _version));
                }
                this.voters = newCollection;
            }
        }
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field grantingVoters was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                if (arrayLength > MessageUtil.MAX_ARRAY_LENGTH) {
                    throw new RuntimeException("Tried to read a collection of size " + arrayLength + ", which exceeds the maximum allowed size of " + MessageUtil.MAX_ARRAY_LENGTH + ".");
                }
                ArrayList<Voter> newCollection = new ArrayList<>(Math.min(arrayLength, MessageUtil.MAX_PREALLOCATED_ARRAY_CAPACITY));
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Voter(_readable, _version));
                }
                this.grantingVoters = newCollection;
            }
        }
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
        _writable.writeInt(leaderId);
        _writable.writeUnsignedVarint(voters.size() + 1);
        for (Voter votersElement : voters) {
            votersElement.write(_writable, _cache, _version);
        }
        _writable.writeUnsignedVarint(grantingVoters.size() + 1);
        for (Voter grantingVotersElement : grantingVoters) {
            grantingVotersElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }

    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(2);
        _size.addBytes(4);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(voters.size() + 1));
            for (Voter votersElement : voters) {
                votersElement.addSize(_size, _cache, _version);
            }
        }
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(grantingVoters.size() + 1));
            for (Voter grantingVotersElement : grantingVoters) {
                grantingVotersElement.addSize(_size, _cache, _version);
            }
        }
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
        if (!(obj instanceof LeaderChangeMessage)) return false;
        LeaderChangeMessage other = (LeaderChangeMessage) obj;
        if (version != other.version) return false;
        if (leaderId != other.leaderId) return false;
        if (this.voters == null) {
            if (other.voters != null) return false;
        } else {
            if (!this.voters.equals(other.voters)) return false;
        }
        if (this.grantingVoters == null) {
            if (other.grantingVoters != null) return false;
        } else {
            if (!this.grantingVoters.equals(other.grantingVoters)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + version;
        hashCode = 31 * hashCode + leaderId;
        hashCode = 31 * hashCode + (voters == null ? 0 : voters.hashCode());
        hashCode = 31 * hashCode + (grantingVoters == null ? 0 : grantingVoters.hashCode());
        return hashCode;
    }

    @Override
    public LeaderChangeMessage duplicate() {
        LeaderChangeMessage _duplicate = new LeaderChangeMessage();
        _duplicate.version = version;
        _duplicate.leaderId = leaderId;
        ArrayList<Voter> newVoters = new ArrayList<Voter>(voters.size());
        for (Voter _element : voters) {
            newVoters.add(_element.duplicate());
        }
        _duplicate.voters = newVoters;
        ArrayList<Voter> newGrantingVoters = new ArrayList<Voter>(grantingVoters.size());
        for (Voter _element : grantingVoters) {
            newGrantingVoters.add(_element.duplicate());
        }
        _duplicate.grantingVoters = newGrantingVoters;
        return _duplicate;
    }

    @Override
    public String toString() {
        return "LeaderChangeMessage("
            + "version=" + version
            + ", leaderId=" + leaderId
            + ", voters=" + MessageUtil.deepToString(voters.iterator())
            + ", grantingVoters=" + MessageUtil.deepToString(grantingVoters.iterator())
            + ")";
    }

    public short version() {
        return this.version;
    }

    public int leaderId() {
        return this.leaderId;
    }

    public List<Voter> voters() {
        return this.voters;
    }

    public List<Voter> grantingVoters() {
        return this.grantingVoters;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public LeaderChangeMessage setVersion(short v) {
        this.version = v;
        return this;
    }

    public LeaderChangeMessage setLeaderId(int v) {
        this.leaderId = v;
        return this;
    }

    public LeaderChangeMessage setVoters(List<Voter> v) {
        this.voters = v;
        return this;
    }

    public LeaderChangeMessage setGrantingVoters(List<Voter> v) {
        this.grantingVoters = v;
        return this;
    }

    public static class Voter implements Message {
        int voterId;
        Uuid voterDirectoryId;
        private List<RawTaggedField> _unknownTaggedFields;

        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("voter_id", Type.INT32, "The ID of the voter."),
                TaggedFieldsSection.of(
                )
            );

        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("voter_id", Type.INT32, "The ID of the voter."),
                new Field("voter_directory_id", Type.UUID, "The directory id of the voter."),
                TaggedFieldsSection.of(
                )
            );

        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
        };

        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 1;

        public Voter(Readable _readable, short _version) {
            read(_readable, _version);
        }

        public Voter() {
            this.voterId = 0;
            this.voterDirectoryId = Uuid.ZERO_UUID;
        }


        @Override
        public short lowestSupportedVersion() {
            return 0;
        }

        @Override
        public short highestSupportedVersion() {
            return 32767;
        }

        @Override
        public final void read(Readable _readable, short _version) {
            this.voterId = _readable.readInt();
            if (_version >= 1) {
                this.voterDirectoryId = _readable.readUuid();
            } else {
                this.voterDirectoryId = Uuid.ZERO_UUID;
            }
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
            _writable.writeInt(voterId);
            if (_version >= 1) {
                _writable.writeUuid(voterDirectoryId);
            } else {
                if (!this.voterDirectoryId.equals(Uuid.ZERO_UUID)) {
                    throw new UnsupportedVersionException("Attempted to write a non-default voterDirectoryId at version " + _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }

        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _size.addBytes(4);
            if (_version >= 1) {
                _size.addBytes(16);
            }
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
            if (!(obj instanceof Voter)) return false;
            Voter other = (Voter) obj;
            if (voterId != other.voterId) return false;
            if (!this.voterDirectoryId.equals(other.voterDirectoryId)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + voterId;
            hashCode = 31 * hashCode + voterDirectoryId.hashCode();
            return hashCode;
        }

        @Override
        public Voter duplicate() {
            Voter _duplicate = new Voter();
            _duplicate.voterId = voterId;
            _duplicate.voterDirectoryId = voterDirectoryId;
            return _duplicate;
        }

        @Override
        public String toString() {
            return "Voter("
                + "voterId=" + voterId
                + ", voterDirectoryId=" + voterDirectoryId.toString()
                + ")";
        }

        public int voterId() {
            return this.voterId;
        }

        public Uuid voterDirectoryId() {
            return this.voterDirectoryId;
        }

        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }

        public Voter setVoterId(int v) {
            this.voterId = v;
            return this;
        }

        public Voter setVoterDirectoryId(Uuid v) {
            this.voterDirectoryId = v;
            return this;
        }
    }
}
