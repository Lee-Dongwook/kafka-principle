package io.github.kafkaprinciple.common.memory;

import io.github.kafkaprinciple.common.metrics.Sensor;
import io.github.kafkaprinciple.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleMemoryPool implements MemoryPool {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final long sizeBytes;
    protected final boolean strict;
    protected final AtomicLong availableMemory;
    protected final int maxSingleAllocationSize;
    protected final AtomicLong startOfNoMemPeriod = new AtomicLong();
    protected volatile Sensor oomTimeSensor;

    public SimpleMemoryPool(long sizeInBytes, int maxSingleAllocationSize, boolean strict, Sensor oomPeriodSensor) {
        if (sizeInBytes <= 0 || maxSingleAllocationBytes <= 0 || maxSingleAllocationBytes > sizeInBytes)
            throw new IllegalArgumentException(
                    "must provide a positive size and max single allocation size smaller than size."
                            + "provided " + sizeInBytes + " and " + maxSingleAllocationBytes + " respectively");

        this.sizeBytes = sizeInBytes;
        this.strict = strict;
        this.availableMemory = new AtomicLong(sizeInBytes);
        this.maxSingleAllocationSize = maxSingleAllocationSize;
        this.oomTimeSensor = oomPeriodSensor;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 1)
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        if (sizeBytes > maxSingleAllocationSize)
            throw new IllegalArgumentException("requested size " + sizeBytes
                    + " is larger than maxSingleAllocationSize " + maxSingleAllocationSize);

        long available;
        boolean success = false;

        long threshold = strict ? sizeBytes : 1;
        while ((available = availableMemory.get()) >= threshold) {
            success = availableMemory.compareAndSet(available, available - sizeBytes);
            if (success)
                break;
        }

        if (success) {
            maybeRecordEndOfDrySpell();
        } else {
            if (oomTimeSensor != null) {
                startOfNoMemPeriod.compareAndSet(0, System.nanoTime());
            }

            log.trace("refused to allocate buffer of size {}", sizeBytes);
            return null;
        }

        ByteBuffer allocated = ByteBuffer.allocate(sizeBytes);
        bufferToBeReturned(allocated);
        return allocated;
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null)
            throw new IllegalArgumentException("provided null buffer");

        bufferToBeReleased(previouslyAllocated);
        availableMemory.addAndGet(previouslyAllocated.capacity());
        maybeRecordEndOfDrySpell();
    }

    @Override
    public long size() {
        return sizeBytes;
    }

    @Override
    public long availableMemory() {
        return availableMemory.get();
    }

    @Override
    public boolean isOutOfMemory() {
        return availableMemory.get() <= 0;
    }

    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        log.trace("allocated buffer of size {} ", justAllocated.capacity());
    }

    protected void bufferToBeReleased(ByteBuffer justReleased) {
        log.trace("released buffer of size {}", justReleased.capacity());
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "SimpleMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes) + " used}";
    }

    protected void maybeRecordEndOfDrySpell() {
        if (oomTimeSensor != null) {
            long startOfDrySpell = startOfNoMemPeriod.getAndSet(0);
            if (startOfDrySpell != 0) {
                oomTimeSensor.record((System.nanoTime() - startOfDrySpell) / 1000000.0);
            }
        }
    }
}
