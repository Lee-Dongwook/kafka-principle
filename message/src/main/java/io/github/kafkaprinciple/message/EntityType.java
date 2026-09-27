
package io.github.kafkaprinciple.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum EntityType {
    @JsonProperty("unknown")
    UNKNOWN(null),

    @JsonProperty("transactionalId")
    TRANSACTIONAL_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("producerId")
    PRODUCER_ID(FieldType.Int64FieldType.INSTANCE),

    @JsonProperty("groupId")
    GROUP_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("topicName")
    TOPIC_NAME(FieldType.StringFieldType.INSTANCE),

    @JsonProperty("brokerId")
    BROKER_ID(FieldType.Int32FieldType.INSTANCE);

    private final FieldType baseType;

    EntityType(FieldType baseType) {
        this.baseType = baseType;
    }

    public void verifyTypeMatches(String fieldName, FieldType type) {
        if (this == UNKNOWN) {
            return;
        }
        if (type instanceof FieldType.ArrayType) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            verifyTypeMatches(fieldName, arrayType.elementType());
        } else {
            if (!type.toString().equals(baseType.toString())) {
                throw new RuntimeException("Field " + fieldName + " has entity type " +
                    name() + ", but field type " + type + ", which does " +
                    "not match.");
            }
        }
    }
}
