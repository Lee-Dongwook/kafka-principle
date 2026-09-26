package io.github.kafkaprinciple.common.utils;


import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


public class Deadline {
    private final long nanoseconds;
    
    public static Deadline fromMonotonicNanoseconds(
        long nanoseconds
    ) {
        return new Deadline(nanoseconds);
    }

    public static Deadline fromDelay(
        Time time,
        long delay,
        TimeUnit timeUnit
    ) {
        if (delay < 0) {
            throw new RuntimeException("Negative delays are not allowed.");
        }

        long nowNs = time.nanoseconds();
        BigInteger deadlineNs = BigInteger.valueOf(nowNs).add(BigInteger.valueOf(timeUnit.toNanos(delay)));
        if (deadlineNs.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 0) {
            return new Deadline(Long.MAX_VALUE);
        } else {
            return new Deadline(deadlineNs.longValue());
        }
    }
    
    private Deadline(long nanoseconds) {
        this.nanoseconds = nanoseconds;
    }

    public long nanoseconds() {
        return nanoseconds;
    }

    @Override 
    public int hashCode() {
        return Objects.hash(nanoseconds);
    }

    @Override 
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(this.getClass()))) return false;
        Deadline other = (Deadline) o;
        return nanoseconds == other.nanoseconds;
    }
}
