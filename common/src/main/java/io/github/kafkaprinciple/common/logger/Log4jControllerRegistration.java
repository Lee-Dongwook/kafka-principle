package io.github.kafkaprinciple.common.logger;

import io.github.kafkaprinciple.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public final class Log4jControllerRegistration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Log4jControllerRegistration.class);

    private static final AtomicBoolean REGISTERED = new AtomicBoolean(false);

    private static final String MBEAN_TYPE = "kafka.Log4jController";
    private static final String MBEAN_NAME = "kafka:type=" + MBEAN_TYPE;

    private Log4jControllerRegistration() {

    }

    public static void register() {
        if (REGISTERED.compareAndSet(false, true)) {
            if (Utils.registerMBean(new LoggingController(), MBEAN_NAME)) {
                LOGGER.info("Registered `{}` MBean", MBEAN_NAME);
            }
        }
    }
}
