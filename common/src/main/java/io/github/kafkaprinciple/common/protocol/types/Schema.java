package io.github.kafkaprinciple.common.protocol.types;

import io.github.kafkaprinciple.common.protocol.types.Type.DocumentedType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Schema extends DocumentedType {
    private static final Object[] NO_VALUES = new Object[0];

    private final BoundField[] fields;
    private final Map<String, BoundField> fieldsByName;
    private final boolean tolerateMissingFieldsWithDefaults;
    private final Struct cachedStruct;

        public Schema(Field... fs) {
        this(false, fs);
    }

        @SuppressWarnings("this-escape")
    public Schema(boolean tolerateMissingFieldsWithDefaults, Field... fs) {
        this.fields = new BoundField[fs.length];
        this.fieldsByName = new HashMap<>();
        this.tolerateMissingFieldsWithDefaults = tolerateMissingFieldsWithDefaults;
        for (int i = 0; i < this.fields.length; i++) {
            Field def = fs[i];
            if (fieldsByName.containsKey(def.name))
                throw new SchemaException("Schema contains a duplicate field: " + def.name);
            this.fields[i] = new BoundField(def, this, i);
            this.fieldsByName.put(def.name, this.fields[i]);
        }
        this.cachedStruct = this.fields.length > 0 ? null : new Struct(this, NO_VALUES);
    }

        @Override
    public void write(ByteBuffer buffer, Object o) {
        Struct r = (Struct) o;
        for (BoundField field : fields) {
            try {
                Object value = field.def.type.validate(r.get(field));
                field.def.type.write(buffer, value);
            } catch (Exception e) {
                throw new SchemaException("Error writing field '" + field.def.name + "': " +
                                          (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
    }

        @Override
    public Struct read(ByteBuffer buffer) {
        if (cachedStruct != null) {
            return cachedStruct;
        }
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            try {
                if (tolerateMissingFieldsWithDefaults) {
                    if (buffer.hasRemaining()) {
                        objects[i] = fields[i].def.type.read(buffer);
                    } else if (fields[i].def.hasDefaultValue) {
                        objects[i] = fields[i].def.defaultValue;
                    } else {
                        throw new SchemaException("Missing value for field '" + fields[i].def.name +
                                "' which has no default value.");
                    }
                } else {
                    objects[i] = fields[i].def.type.read(buffer);
                }
            } catch (Exception e) {
                throw new SchemaException("Error reading field '" + fields[i].def.name + "': " +
                                          (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
        return new Struct(this, objects);
    }

        @Override
    public int sizeOf(Object o) {
        int size = 0;
        Struct r = (Struct) o;
        for (BoundField field : fields) {
            try {
                size += field.def.type.sizeOf(r.get(field));
            } catch (Exception e) {
                throw new SchemaException("Error computing size for field '" + field.def.name + "': " +
                        (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
        return size;
    }

        public int numFields() {
        return this.fields.length;
    }

        public BoundField get(int slot) {
        return this.fields[slot];
    }

        public BoundField get(String name) {
        return this.fieldsByName.get(name);
    }

        public BoundField[] fields() {
        return this.fields;
    }

    protected boolean tolerateMissingFieldsWithDefaults() {
        return this.tolerateMissingFieldsWithDefaults;
    }

    @Override
    public String leftBracket() {
        return "{";
    }

    @Override
    public String rightBracket() {
        return "}";
    }

        @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.fields.length; i++) {
            b.append(this.fields[i].toString());
            if (i < this.fields.length - 1)
                b.append(',');
        }
        b.append("}");
        return b.toString();
    }

    @Override
    public Struct validate(Object item) {
        try {
            Struct struct = (Struct) item;
            for (BoundField field : fields) {
                try {
                    field.def.type.validate(struct.get(field));
                } catch (SchemaException e) {
                    throw new SchemaException("Invalid value for field '" + field.def.name + "': " + e.getMessage());
                }
            }
            return struct;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a Struct.");
        }
    }

    @Override
    public String typeName() {
        return "STRUCT";
    }

    @Override
    public String documentation() {
        return "A struct is named by a string with a capitalized first letter and consists of one or more fields. " +
            "It represents a composite object encoded as the serialization of each field in the order they are defined." +
            "In protocol documentation a struct containing multiple fields is enclosed by " +
            leftBracket() + " and " + rightBracket() + ".";
    }

    public void walk(Visitor visitor) {
        Objects.requireNonNull(visitor, "visitor must be non-null");
        handleNode(this, visitor);
    }

    private static void handleNode(Type node, Visitor visitor) {
        if (node instanceof Schema) {
            Schema schema = (Schema) node;
            visitor.visit(schema);
            for (BoundField f : schema.fields())
                handleNode(f.def.type, visitor);
        } else if (node.isArray()) {
            visitor.visit(node);
            handleNode(node.arrayElementType().get(), visitor);
        } else {
            visitor.visit(node);
        }
    }

        public abstract static class Visitor {
        public void visit(Schema schema) {}
        public void visit(Type field) {}
    }
}
