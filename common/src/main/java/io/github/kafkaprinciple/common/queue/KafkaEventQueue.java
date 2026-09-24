package io.github.kafkaprinciple.common.queue;

import java.util.OptionalLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Event execution primitive. The full broker scheduler will be added with the
 * controller runtime; this type currently provides the API's safe event
 * dispatch behavior.
 */
public final class KafkaEventQueue implements EventQueue {
    public static final String EVENT_HANDLER_THREAD_SUFFIX = "event-handler";

    private static class EventContext {
        private final Event event;
        private final EventInsertionType insertionType;
        private EventContext prev = this;
        private EventContext next = this;
        private OptionalLong deadlineNs = OptionalLong.empty();
        private String tag;

        EventContext(Event event, EventInsertionType insertionType, String tag) {
            this.event = event;
            this.insertionType = insertionType;
            this.tag = tag;
        }

        void insertAfter(EventContext other) {
            this.next.prev = other;
            other.next = this.next;
            other.prev = this;
            this.next = other;
        }

        void insertBefore(EventContext other) {
            this.prev.next = other;
            other.prev = this.prev;
            other.next = this;
            this.prev = other;
        }

        void remove() {
            this.prev.next = this.next;
            this.next.prev = this.prev;
            this.prev = this;
            this.next = this;
        }

        boolean isSingleton() {
            return prev == this && next == this;
        }

        boolean run(Logger log, Throwable exceptionToDeliver) {
            if(exceptionToDeliver == null) {
                try {
                    event.run();
                } catch (InterruptedException e) {
                    log.warning("Interrupted while running event.");
                    return true;
                } catch(Throwable e) {
                    log.log(Level.FINE, "Got exception while running " + event + ".", e);
                }
            }

            if (exceptionToDeliver != null) {
                completeWithException(log, exceptionToDeliver);
            }

            return Thread.currentThread().isInterrupted();
        }

        void completeWithException(Logger log, Throwable t) {
            try {
                event.handleException(t);
            } catch (Exception e) {
                log.log(Level.SEVERE, "Unexpected exception in handleException", e);
            }
        }
    }

    private final Logger logger = Logger.getLogger(KafkaEventQueue.class.getName());

    @Override
    public void enqueue(
        EventInsertionType insertionType,
        String tag,
        java.util.function.UnaryOperator<OptionalLong> deadlineNsCalculator,
        Event event
    ) {
        if (event == null) {
            throw new IllegalArgumentException("event cannot be null");
        }
        new EventContext(event, insertionType, tag).run(logger, null);
    }

    @Override
    public void beginShutdown(String source) {
        // This minimal queue executes events synchronously, so there is no worker to stop.
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void cancelDeferred(String tag) {
        // Deferred scheduling is not enabled until the controller runtime is implemented.
    }

    @Override
    public void close() {
        beginShutdown("close");
    }
}
