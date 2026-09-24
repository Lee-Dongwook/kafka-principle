package io.github.kafkaprinciple.common.queue;

import java.util.OptionalLong;
import java.util.function.UnaryOperator;

public interface EventQueue extends AutoCloseable {
    
    enum EventInsertionType {
        PREPEND,
        APPEND,
        DEFERRED
    }

    interface Event {
        void run() throws Exception;
        default void handleException(Throwable e) {}
    }

    void enqueue(
        EventInsertionType insertionType, 
        String tag, 
        UnaryOperator<OptionalLong> deadlineNsCalculator, 
        Event event
    );

    void beginShutdown(String source);

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }

    default void wakeup() {}

    void close() throws InterruptionException;

    void cancelDeferred(String tag);

    class NoDeadlineFunction implements UnaryOperator<OptionalLong> {
        public static final NoDeadlineFunction INSTANCE = new NoDeadlineFunction();

        private NoDeadlineFunction() {

        }

        @Override
        public OptionalLong apply(OptionalLong ignored) {
            return OptionalLong.empty();
        }
    }

    class DeadlineFunction implements UnaryOperator<OptionalLong> {
        private final long deadlineNs;

        public DeadlineFunction(long deadlineNs) {
            this.deadlineNs = deadlineNs;
        }

        @Override
        public OptionalLong apply(OptionalLong ignored) {
            return OptionalLong.of(deadlineNs);
        }
    }

    class EarliestDeadlineFunction implements UnaryOperator<OptionalLong> {
        private final long newDeadlineNs;

        public EarliestDeadlineFunction(long newDeadlineNs) {
            this.newDeadlineNs = newDeadlineNs;
        }

        @Override
        public OptionalLong apply(OptionalLong prevDeadlineNs) {
            if (prevDeadlineNs.isEmpty()) {
                return OptionalLong.of(newDeadlineNs);
            } else if (prevDeadlineNs.getAsLong() < newDeadlineNs) {
                return prevDeadlineNs;
            } else {
                return OptionalLong.of(newDeadlineNs);
            }
        }
    }

    class VoidEvent implements Event {
        public static final VoidEvent INSTANCE = new VoidEvent();
        
        private VoidEvent() {
            
        }
        
        @Override
        public void run() throws Exception {
        }
    }

    default void prepend(Event event) {
        enqueue(EventInsertionType.PREPEND, null, NoDeadlineFunction.INSTANCE, event);
    }

    default void append(Event event) {
        enqueue(EventInsertionType.APPEND, null, NoDeadlineFunction.INSTANCE, event);
    }

    default void appendWithDeadline(long deadlineNs, Event event) {
        enqueue(EventInsertionType.APPEND, null, new DeadlineFunction(deadlineNs), event);
    }

    default void scheduleDeferred(String tag,
                                  UnaryOperator<OptionalLong> deadlineNsCalculator,
                                  Event event) {
        enqueue(EventInsertionType.DEFERRED, tag, deadlineNsCalculator, event);
    }
}
