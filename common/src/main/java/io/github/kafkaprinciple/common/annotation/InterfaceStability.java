package io.github.kafkaprinciple.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@InterfaceAudience.Public
public class InterfaceStability {
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Stable {
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Evolving {
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Unstable {
    }
}
