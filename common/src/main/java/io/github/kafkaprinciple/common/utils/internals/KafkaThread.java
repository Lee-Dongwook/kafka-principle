package io.github.kafkaprinciple.common.utils.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(KafkaThread.class);

    public static KafkaThread daemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, true);
    }

    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, false);
    }

    @SuppressWarnings("this-escape")
    public KafkaThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    @SuppressWarnings("this-escape")
    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }

    private void configureThread(final String name, boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
    }
}
