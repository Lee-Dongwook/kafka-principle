
package io.github.kafkaprinciple.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

final class SchemaGenerator {
        static class MessageInfo {
                private final Versions versions;

                private final TreeMap<Short, CodeBuffer> schemaForVersion;

        MessageInfo(Versions versions) {
            this.versions = versions;
            this.schemaForVersion = new TreeMap<>();
        }
    }

        private final HeaderGenerator headerGenerator;

        private final StructRegistry structRegistry;

        private final Map<String, MessageInfo> messages;

        private Versions messageFlexibleVersions;

    SchemaGenerator(HeaderGenerator headerGenerator, StructRegistry structRegistry) {
        this.headerGenerator = headerGenerator;
        this.structRegistry = structRegistry;
        this.messages = new HashMap<>();
    }

    void generateSchemas(MessageSpec message) {
        this.messageFlexibleVersions = message.flexibleVersions();

        for (Iterator<StructSpec> iter = structRegistry.commonStructs(); iter.hasNext(); ) {
            StructSpec struct = iter.next();
            generateSchemas(struct.name(), struct, message.struct().versions());
        }

        generateSchemas(message.dataClassName(), message.struct(), message.struct().versions());
    }

    void generateSchemas(String className, StructSpec struct, Versions parentVersions) {
        Versions versions = parentVersions.intersect(struct.versions());
        MessageInfo messageInfo = messages.get(className);
        if (messageInfo != null) {
            return;
        }
        messageInfo = new MessageInfo(versions);
        messages.put(className, messageInfo);
        for (FieldSpec field : struct.fields()) {
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                generateSchemas(arrayType.elementType().toString(), structRegistry.findStruct(field), versions);
            } else if (field.type().isStruct()) {
                generateSchemas(field.type().toString(), structRegistry.findStruct(field), versions);
            }
        }
        CodeBuffer prev = null;
        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            CodeBuffer cur = new CodeBuffer();
            generateSchemaForVersion(struct, v, cur);
            if (!cur.equals(prev)) {
                messageInfo.schemaForVersion.put(v, cur);
            }
            prev = cur;
        }
    }

    private void generateSchemaForVersion(StructSpec struct,
                                          short version,
                                          CodeBuffer buffer) {
        int lastValidIndex = struct.fields().size() - 1;
        while (true) {
            if (lastValidIndex < 0) {
                break;
            }
            FieldSpec field = struct.fields().get(lastValidIndex);
            if ((!field.taggedVersions().contains(version)) &&
                    field.versions().contains(version)) {
                break;
            }
            lastValidIndex--;
        }
        int finalLine = lastValidIndex;
        if (messageFlexibleVersions.contains(version)) {
            finalLine++;
        }

        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS);
        buffer.printf("new Schema(%n");
        buffer.incrementIndent();
        for (int i = 0; i <= lastValidIndex; i++) {
            FieldSpec field = struct.fields().get(i);
            if ((!field.versions().contains(version)) ||
                    field.taggedVersions().contains(version)) {
                continue;
            }
            Versions fieldFlexibleVersions =
                field.flexibleVersions().orElse(messageFlexibleVersions);
            headerGenerator.addImport(MessageGenerator.FIELD_CLASS);
            buffer.printf("new Field(\"%s\", %s, \"%s\")%s%n",
                field.snakeCaseName(),
                fieldTypeToSchemaType(field, version, fieldFlexibleVersions),
                field.about(),
                i == finalLine ? "" : ",");
        }
        if (messageFlexibleVersions.contains(version)) {
            generateTaggedFieldsSchemaForVersion(struct, version, buffer);
        }
        buffer.decrementIndent();
        buffer.printf(");%n");
    }

    private void generateTaggedFieldsSchemaForVersion(StructSpec struct,
            short version, CodeBuffer buffer) {
        headerGenerator.addStaticImport(MessageGenerator.TAGGED_FIELDS_SECTION_CLASS);

        int lastValidIndex = struct.fields().size() - 1;
        while (true) {
            if (lastValidIndex < 0) {
                break;
            }
            FieldSpec field = struct.fields().get(lastValidIndex);
            if ((field.taggedVersions().contains(version)) &&
                field.versions().contains(version)) {
                break;
            }
            lastValidIndex--;
        }

        buffer.printf("TaggedFieldsSection.of(%n");
        buffer.incrementIndent();
        for (int i = 0; i <= lastValidIndex; i++) {
            FieldSpec field = struct.fields().get(i);
            if ((!field.versions().contains(version)) ||
                    (!field.taggedVersions().contains(version))) {
                continue;
            }
            headerGenerator.addImport(MessageGenerator.FIELD_CLASS);
            Versions fieldFlexibleVersions =
                field.flexibleVersions().orElse(messageFlexibleVersions);
            buffer.printf("%d, new Field(\"%s\", %s, \"%s\")%s%n",
                field.tag().get(),
                field.snakeCaseName(),
                fieldTypeToSchemaType(field, version, fieldFlexibleVersions),
                field.about(),
                i == lastValidIndex ? "" : ",");
        }
        buffer.decrementIndent();
        buffer.printf(")%n");
    }

    private String fieldTypeToSchemaType(FieldSpec field,
                                         short version,
                                         Versions fieldFlexibleVersions) {
        return fieldTypeToSchemaType(field.type(),
            field.nullableVersions().contains(version),
            version,
            fieldFlexibleVersions,
            field.zeroCopy());
    }

    private String fieldTypeToSchemaType(FieldType type,
                                         boolean nullable,
                                         short version,
                                         Versions fieldFlexibleVersions,
                                         boolean zeroCopy) {
        if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.BOOLEAN";
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT8";
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT16";
        } else if (type instanceof FieldType.Uint16FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.UINT16";
        } else if (type instanceof FieldType.Uint32FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.UNSIGNED_INT32";
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT32";
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT64";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.UUID";
        } else if (type instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.FLOAT64";
        } else if (type instanceof FieldType.StringFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (fieldFlexibleVersions.contains(version)) {
                return nullable ? "Type.COMPACT_NULLABLE_STRING" : "Type.COMPACT_STRING";
            } else {
                return nullable ? "Type.NULLABLE_STRING" : "Type.STRING";
            }
        } else if (type instanceof FieldType.BytesFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (fieldFlexibleVersions.contains(version)) {
                return nullable ? "Type.COMPACT_NULLABLE_BYTES" : "Type.COMPACT_BYTES";
            } else {
                return nullable ? "Type.NULLABLE_BYTES" : "Type.BYTES";
            }
        } else if (type.isRecords()) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (fieldFlexibleVersions.contains(version)) {
                return nullable ? "Type.COMPACT_NULLABLE_RECORDS" : "Type.COMPACT_RECORDS";
            } else {
                return nullable ? "Type.NULLABLE_RECORDS" : "Type.RECORDS";
            }
        } else if (type.isArray()) {
            if (fieldFlexibleVersions.contains(version)) {
                headerGenerator.addImport(MessageGenerator.COMPACT_ARRAYOF_CLASS);
                FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                String prefix = nullable ? "CompactArrayOf.nullable" : "new CompactArrayOf";
                return String.format("%s(%s)", prefix,
                        fieldTypeToSchemaType(arrayType.elementType(), false, version, fieldFlexibleVersions, false));

            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYOF_CLASS);
                FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                String prefix = nullable ? "ArrayOf.nullable" : "new ArrayOf";
                return String.format("%s(%s)", prefix,
                        fieldTypeToSchemaType(arrayType.elementType(), false, version, fieldFlexibleVersions, false));
            }
        } else if (type.isStruct()) {
            if (nullable) {
                headerGenerator.addImport(MessageGenerator.NULLABLE_SCHEMA_CLASS);
            }
            String schemaType = String.format("%s.SCHEMA_%d", type,
                floorVersion(type.toString(), version));
            return nullable ? String.format("new NullableSchema(%s)", schemaType) : schemaType;
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
    }

        private short floorVersion(String className, short v) {
        MessageInfo message = messages.get(className);
        return message.schemaForVersion.floorKey(v);
    }

        void writeSchema(String className, CodeBuffer buffer) {
        MessageInfo messageInfo = messages.get(className);
        Versions versions = messageInfo.versions;

        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            CodeBuffer declaration = messageInfo.schemaForVersion.get(v);
            if (declaration == null) {
                buffer.printf("public static final Schema SCHEMA_%d = SCHEMA_%d;%n", v, v - 1);
            } else {
                buffer.printf("public static final Schema SCHEMA_%d =%n", v);
                buffer.incrementIndent();
                declaration.write(buffer);
                buffer.decrementIndent();
            }
            buffer.printf("%n");
        }
        buffer.printf("public static final Schema[] SCHEMAS = new Schema[] {%n");
        buffer.incrementIndent();
        for (short v = 0; v < versions.lowest(); v++) {
            buffer.printf("null%s%n", (v == versions.highest()) ? "" : ",");
        }
        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            buffer.printf("SCHEMA_%d%s%n", v, (v == versions.highest()) ? "" : ",");
        }
        buffer.decrementIndent();
        buffer.printf("};%n");
        buffer.printf("%n");

        buffer.printf("public static final short LOWEST_SUPPORTED_VERSION = %d;%n", versions.lowest());
        buffer.printf("public static final short HIGHEST_SUPPORTED_VERSION = %d;%n", versions.highest());
        buffer.printf("%n");
    }
}
