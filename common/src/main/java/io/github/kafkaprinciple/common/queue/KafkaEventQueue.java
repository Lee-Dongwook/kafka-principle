package io.github.kafkaprinciple.common.queue;



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
                } catch(InterruptionException e) {
                    log.warn("Interrupted while running event.")
                    return true;
                } catch(Throwable e) {
                    log.debug("Got exception while running {}.", event, e);
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
                log.error("Unexpected exception in handleException", e);
            }
        }
    }
}
