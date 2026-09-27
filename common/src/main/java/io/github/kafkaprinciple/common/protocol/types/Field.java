package io.github.kafkaprinciple.common.protocol.types;

public class Field {
    public final String name;
    public final String docString;
    public final Type type;
    public final boolean hasDefaultValue;
    public final Object defaultValue;

    public Field(String name, Type type, String docString, boolean hasDefaultValue, Object defaultValue) {
        this.name = name;
        this.docString = docString;
        this.type = type;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;

        if (hasDefaultValue)
            type.validate(defaultValue);
    }

    public Field(String name, Type type, String docString) {
        this(name, type, docString, false, null);
    }

    public Field(String name, Type type, String docString, Object defaultValue) {
        this(name, type, docString, true, defaultValue);
    }

    public Field(String name, Type type) {
        this(name, type, null, false, null);
    }

    public static class TaggedFieldsSection extends Field {
        private static final String NAME = "_tagged_fields";
        private static final String DOC_STRING = "The tagged fields";

                public static TaggedFieldsSection of(Object... fields) {
            return new TaggedFieldsSection(TaggedFields.of(fields));
        }

        public TaggedFieldsSection(Type type) {
            super(NAME, type, DOC_STRING, false, null);
        }
    }
}
