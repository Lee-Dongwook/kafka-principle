package io.github.kafkaprinciple.common.utils;

class SystemTime implements Time {
    private static final SystemTime SYSTEM_TIME = new SystemTime();

    public static SystemTime getSystemTime() {
        return SYSTEM_TIME;
    }

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    private SystemTime() {

    }
}
