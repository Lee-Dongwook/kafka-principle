
package io.github.kafkaprinciple.message;

import java.io.BufferedWriter;
import java.io.IOException;

public interface TypeClassGenerator {
        String outputName();

        void registerMessageType(MessageSpec spec);

        void generateAndWrite(BufferedWriter writer) throws IOException;
}
