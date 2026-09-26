package io.github.kafkaprinciple.common.utils;

public class Timer {
    private final Time time;
    private long startMs;
    private long currentTimeMs;
    private long deadlineMs;
    private long timeoutMs;

    Timer(Time time, long timeoutMs) {
        this.time = time;
        update();
        reset(timeoutMs);
    }

    public boolean isExpired() {
        return currentTimeMs >= deadlineMs;
    }

    public long isExpiredBy() {
        return Math.max(0, currentTimeMs - deadlineMs);
    }

    public boolean notExpired() {
        return !isExpired();
    }

    public void updateAndReset(long timeoutMs) {
        update();
        reset(timeoutMs);
    }

    public void reset(long timeoutMs) {
        if (timeoutMs < 0)
            throw new IllegalArgumentException("Invalid negative timeout " + timeoutMs);

        this.timeoutMs = timeoutMs;
        this.startMs = this.currentTimeMs;

        if (currentTimeMs > Long.MAX_VALUE - timeoutMs) {
            this.deadlineMs = Long.MAX_VALUE;
        } else {
            this.deadlineMs = currentTimeMs + timeoutMs;
        }
    }
    
    public void resetDeadline(long deadlineMs) {
        if (deadlineMs < 0)
            throw new IllegalArgumentException("Invalid negative deadline " + deadlineMs);

        this.timeoutMs = Math.max(0, deadlineMs - this.currentTimeMs);
        this.startMs = this.currentTimeMs;
        this.deadlineMs = deadlineMs;
    }

    public void update() {
        update(time.milliseconds());
    }

    public void update(long currentTimeMs) {
        this.currentTimeMs = Math.max(currentTimeMs, this.currentTimeMs);
    }

    public long remainingMs() {
        return Math.max(0, deadlineMs - currentTimeMs);
    }

    public long currentTimeMs() {
        return currentTimeMs;
    }
    
    public long elapsedMs() {
        return currentTimeMs - startMs;
    }   

    public long timeoutMs() {
        return timeoutMs;
    }

    public void sleep(long durationMs) {
        long sleepDurationMs = Math.min(durationMs, remainingMs());
        time.sleep(sleepDurationMs);
        update();
    }
}
