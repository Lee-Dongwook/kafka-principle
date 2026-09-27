package io.github.kafkaprinciple.common.utils.internals;

public class Exit {

    @FunctionalInterface
    public interface Procedure {
        void execute(int statusCode, String message);
    }

    @FunctionalInterface
    public interface ShutdownHookAdder {
        void addShutdownHook(String name, Runnable runnable);
    }

    private static final Procedure DEFAULT_HALT_PROCEDURE = (statusCode, message) -> Runtime.getRuntime()
            .halt(statusCode);

    private static final Procedure DEFAULT_EXIT_PROCEDURE = (statusCode, message) -> System.exit(statusCode);

    private static final ShutdownHookAdder DEFAULT_SHUTDOWN_HOOK_ADDER = (name, runnable) -> {
        if (name != null) {
            Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(runnable));
        }
    };

    private static final Procedure NOOP_HALT_PROCEDURE = (statusCode, message) -> {
        throw new IllegalStateException(
                "Halt called after resetting procedures; possible race condition present in test");
    };

    private static final Procedure NOOP_EXIT_PROCEDURE = (statusCode, message) -> {
        throw new IllegalStateException(
                "Exit called after resetting procedures; possible race condition present in test");
    };

    private static volatile Procedure exitProcedure = DEFAULT_EXIT_PROCEDURE;
    private static volatile Procedure haltProcedure = DEFAULT_HALT_PROCEDURE;
    private static volatile ShutdownHookAdder shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;

    public static void exit(int statusCode) {
        exit(statusCode, null);
    }

    public static void exit(int statusCode, String message) {
        exitProcedure.execute(statusCode, message);
    }

    public static void halt(int statusCode) {
        halt(statusCode, null);
    }

    public static void halt(int statusCode, String message) {
        haltProcedure.execute(statusCode, message);
    }

    public static void addShutdownHook(String name, Runnable runnable) {
        shutdownHookAdder.addShutdownHook(name, runnable);
    }

    public static void setExitProcedure(Procedure procedure) {
        exitProcedure = procedure;
    }

    public static void setHaltProcedure(Procedure procedure) {
        haltProcedure = procedure;
    }

    public static void setShutdownHookAdder(ShutdownHookAdder shutdownHookAdder) {
        Exit.shutdownHookAdder = shutdownHookAdder;
    }

    public static void resetExitProcedure() {
        exitProcedure = NOOP_EXIT_PROCEDURE;
    }

    public static void resetHaltProcedure() {
        haltProcedure = NOOP_HALT_PROCEDURE;
    }

    public static void resetShutdownHookAdder() {
        shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;
    }
}
