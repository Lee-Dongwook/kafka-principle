
package io.github.kafkaprinciple.message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public final class StructRegistry {
    private final Map<String, StructInfo> structs;
    private final Set<String> commonStructNames;

    static class StructInfo {
                private final StructSpec spec;

                private final Versions parentVersions;

        StructInfo(StructSpec spec, Versions parentVersions) {
            this.spec = spec;
            this.parentVersions = parentVersions;
        }

        public StructSpec spec() {
            return spec;
        }

        public Versions parentVersions() {
            return parentVersions;
        }
    }

    public StructRegistry() {
        this.structs = new TreeMap<>();
        this.commonStructNames = new TreeSet<>();
    }

        public void register(MessageSpec message) throws Exception {
        for (StructSpec struct : message.commonStructs()) {
            if (!MessageGenerator.firstIsCapitalized(struct.name())) {
                throw new RuntimeException("Can't process structure " + struct.name() +
                        ": the first letter of structure names must be capitalized.");
            }
            if (structs.containsKey(struct.name())) {
                throw new RuntimeException("Common struct " + struct.name() + " was specified twice.");
            }
            structs.put(struct.name(), new StructInfo(struct, struct.versions()));
            commonStructNames.add(struct.name());
        }
        addStructSpecs(message.validVersions(), message.fields());
    }

    private void addStructSpecs(Versions parentVersions, List<FieldSpec> fields) {
        for (FieldSpec field : fields) {
            String typeName = null;
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                typeName = arrayType.elementName();
            } else if (field.type().isStruct()) {
                FieldType.StructType structType = (FieldType.StructType) field.type();
                typeName = structType.typeName();
            }
            if (typeName != null) {
                if (commonStructNames.contains(typeName)) {
                    if (!field.fields().isEmpty()) {
                        throw new RuntimeException("Can't re-specify the common struct " +
                                typeName + " as an inline struct.");
                    }
                } else if (structs.containsKey(typeName)) {
                    throw new RuntimeException("Struct " + typeName +
                        " was specified twice.");
                } else {
                    StructSpec spec = new StructSpec(typeName,
                        field.versions().toString(),
                        Versions.NONE_STRING,
                        field.fields());
                    structs.put(typeName, new StructInfo(spec, parentVersions));
                }

                addStructSpecs(parentVersions.intersect(field.versions()), field.fields());
            }
        }
    }

        public StructSpec findStruct(FieldSpec field) {
        String structFieldName;
        if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            structFieldName = arrayType.elementName();
        } else if (field.type().isStruct()) {
            FieldType.StructType structType = (FieldType.StructType) field.type();
            structFieldName = structType.typeName();
        } else {
            throw new RuntimeException("Field " + field.name() +
                    " cannot be treated as a structure.");
        }
        return findStruct(structFieldName);
    }

    public StructSpec findStruct(String structFieldName) {
        StructInfo structInfo = structs.get(structFieldName);
        if (structInfo == null) {
            throw new RuntimeException("Unable to locate a specification for the structure " +
                    structFieldName);
        }
        return structInfo.spec;
    }

        public boolean isStructArrayWithKeys(FieldSpec field) {
        if (!field.type().isArray()) {
            return false;
        }
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
        if (!arrayType.isStructArray()) {
            return false;
        }
        StructInfo structInfo = structs.get(arrayType.elementName());
        if (structInfo == null) {
            throw new RuntimeException("Unable to locate a specification for the structure " +
                    arrayType.elementName());
        }
        return structInfo.spec.hasKeys();
    }

    Set<String> commonStructNames() {
        return commonStructNames;
    }

        Iterator<StructSpec> commonStructs() {
        return new Iterator<StructSpec>() {
            private final Iterator<String> iter = commonStructNames.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public StructSpec next() {
                return structs.get(iter.next()).spec;
            }
        };
    }

    Iterator<StructInfo> structs() {
        return structs.values().iterator();
    }
}
