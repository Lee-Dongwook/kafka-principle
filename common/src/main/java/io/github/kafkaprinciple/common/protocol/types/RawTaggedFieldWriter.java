
package io.github.kafkaprinciple.common.protocol.types;

import io.github.kafkaprinciple.common.protocol.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class RawTaggedFieldWriter {
    private static final RawTaggedFieldWriter EMPTY_WRITER =
        new RawTaggedFieldWriter(new ArrayList<>(0));

    private final List<RawTaggedField> fields;
    private final ListIterator<RawTaggedField> iter;
    private int prevTag;

    public static RawTaggedFieldWriter forFields(List<RawTaggedField> fields) {
        if (fields == null) {
            return EMPTY_WRITER;
        }
        return new RawTaggedFieldWriter(fields);
    }

    private RawTaggedFieldWriter(List<RawTaggedField> fields) {
        this.fields = fields;
        this.iter = this.fields.listIterator();
        this.prevTag = -1;
    }

    public int numFields() {
        return fields.size();
    }

    public void writeRawTags(Writable writable, int nextDefinedTag) {
        while (iter.hasNext()) {
            RawTaggedField field = iter.next();
            int tag = field.tag();
            if (tag >= nextDefinedTag) {
                if (tag == nextDefinedTag) {
                    throw new RuntimeException("Attempted to use tag " + tag + " as an " +
                        "undefined tag.");
                }
                iter.previous();
                return;
            }
            if (tag <= prevTag) {
                throw new RuntimeException("Invalid raw tag field list: tag " + tag +
                    " comes after tag " + prevTag + ", but is not higher than it.");
            }
            writable.writeUnsignedVarint(field.tag());
            writable.writeUnsignedVarint(field.data().length);
            writable.writeByteArray(field.data());
            prevTag = tag;
        }
    }
}
