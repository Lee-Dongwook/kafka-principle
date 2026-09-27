
package io.github.kafkaprinciple.message;

import java.io.BufferedWriter;

public interface MessageClassGenerator {
        String outputName(MessageSpec spec);

        void generateAndWrite(MessageSpec spec, BufferedWriter writer) throws Exception;
}
