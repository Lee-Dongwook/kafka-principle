package io.github.kafkaprinciple.common.protocol.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class Struct {
    private final Schema schema;
    private final Object[] values;

    Struct(Schema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public Struct(Schema schema) {
        this.schema = schema;
        this.values = new Object[this.schema.numFields()];
    }

        public Schema schema() {
        return this.schema;
    }

        private Object getFieldOrDefault(BoundField field) {
        Object value = this.values[field.index];
        if (value != null)
            return value;
        else if (field.def.hasDefaultValue)
            return field.def.defaultValue;
        else if (field.def.type.isNullable())
            return null;
        else
            throw new SchemaException("Missing value for field '" + field.def.name + "' which has no default value.");
    }

        public Object get(BoundField field) {
        validateField(field);
        return getFieldOrDefault(field);
    }

        public Object get(String name) {
        BoundField field = schema.get(name);
        if (field == null)
            throw new SchemaException("No such field: " + name);
        return getFieldOrDefault(field);
    }

        public boolean hasField(String name) {
        return schema.get(name) != null;
    }

    public Short getShort(String name) {
        return (Short) get(name);
    }

    public Integer getInt(String name) {
        return (Integer) get(name);
    }

    public Long getLong(String name) {
        return (Long) get(name);
    }

    public Object[] getArray(String name) {
        return (Object[]) get(name);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    public ByteBuffer getBytes(String name) {
        Object result = get(name);
        if (result instanceof byte[])
            return ByteBuffer.wrap((byte[]) result);
        return (ByteBuffer) result;
    }

        public Struct set(BoundField field, Object value) {
        validateField(field);
        this.values[field.index] = value;
        return this;
    }

        public Struct set(String name, Object value) {
        BoundField field = this.schema.get(name);
        if (field == null)
            throw new SchemaException("Unknown field: " + name);
        this.values[field.index] = value;
        return this;
    }

        public Struct instance(BoundField field) {
        validateField(field);
        if (field.def.type instanceof Schema) {
            return new Struct((Schema) field.def.type);
        } else if (field.def.type.isArray()) {
            return new Struct((Schema) field.def.type.arrayElementType().get());
        } else {
            throw new SchemaException("Field '" + field.def.name + "' is not a container type, it is of type " + field.def.type);
        }
    }

        public Struct instance(String field) {
        return instance(schema.get(field));
    }

    public Struct instance(Field field) {
        return instance(schema.get(field.name));
    }

        public void clear() {
        Arrays.fill(this.values, null);
    }

        public int sizeOf() {
        return this.schema.sizeOf(this);
    }

        public void writeTo(ByteBuffer buffer) {
        this.schema.write(buffer, this);
    }

        private void validateField(BoundField field) {
        Objects.requireNonNull(field, "`field` must be non-null");
        if (this.schema != field.schema)
            throw new SchemaException("Attempt to access field '" + field.def.name + "' from a different schema instance.");
        if (field.index > values.length)
            throw new SchemaException("Invalid field index: " + field.index);
    }

        public void validate() {
        this.schema.validate(this);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.values.length; i++) {
            BoundField f = this.schema.get(i);
            b.append(f.def.name);
            b.append('=');
            if (f.def.type.isArray() && this.values[i] != null) {
                Object[] arrayValue = (Object[]) this.values[i];
                b.append('[');
                for (int j = 0; j < arrayValue.length; j++) {
                    b.append(arrayValue[j]);
                    if (j < arrayValue.length - 1)
                        b.append(',');
                }
                b.append(']');
            } else
                b.append(this.values[i]);
            if (i < this.values.length - 1)
                b.append(',');
        }
        b.append('}');
        return b.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        for (int i = 0; i < this.values.length; i++) {
            BoundField f = this.schema.get(i);
            if (f.def.type.isArray()) {
                if (this.get(f) != null) {
                    Object[] arrayObject = (Object[]) this.get(f);
                    for (Object arrayItem: arrayObject)
                        result = prime * result + arrayItem.hashCode();
                }
            } else {
                Object field = this.get(f);
                if (field != null) {
                    result = prime * result + field.hashCode();
                }
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Struct other = (Struct) obj;
        if (schema != other.schema)
            return false;
        for (int i = 0; i < this.values.length; i++) {
            BoundField f = this.schema.get(i);
            boolean result;
            if (f.def.type.isArray()) {
                result = Arrays.equals((Object[]) this.get(f), (Object[]) other.get(f));
            } else {
                Object thisField = this.get(f);
                Object otherField = other.get(f);
                result = Objects.equals(thisField, otherField);
            }
            if (!result)
                return false;
        }
        return true;
    }
}
