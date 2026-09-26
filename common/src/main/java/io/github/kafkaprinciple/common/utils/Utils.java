package io.github.kafkaprinciple.common.utils;

public final class Utils {
    private Utils() {
    }
    
    //TODO

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
