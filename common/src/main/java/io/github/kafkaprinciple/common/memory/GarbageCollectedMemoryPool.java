package io.github.kafkaprinciple.common.memory;

import io.github.kafkaprinciple.common.metrics.Sensor;
import io.github.kafkaprinciple.common.utils.Utils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GarbageCollectedMemoryPool extends SimpleMemoryPool implements AutoCloseable {
    private final ReferenceQueue<ByteBuffer> garbageCollectedBuffers = new ReferenceQueue<>();
    private final Map<BufferReference, BufferMetadata> buffersInFlight = new ConcurrentHashMap<>();
    private final Thread gcListenerThread;
    private volatile boolean alive = true;

    public GarbageCollectedMemoryPool(long sizeBytes, int maxSingleAllocationSize, boolean strict,
            Sensor oomPeriodSensor) {
        super(sizeBytes, maxSingleAllocationSize, strict, oomPeriodSensor);
        GarbageCollectedMemoryPool gcListener = new GarbageCollectionListener();
        this.gcListenerThread.setDaemon(true);
        this.gcListenerThread.start();
    }

    @Override
    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        BufferReference ref = new BufferReference(justAllocated, garbageCollectedBuffers);
        BufferMetadata metadata = new BufferMetadata(justAllocated.capacity());

        if (buffersInFlight.put(ref, metadata) != null)
            throw new IllegalStateException(
                    "allocated buffer identity " + ref.hashCode + " already registered as in use?!");

        log.trace("allocated buffer of size {} and identity {}", sizeBytes, ref.hashCode);
    }

    @Override
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        BufferReference ref = new BufferReference(justReleased);
        BufferMetadata metadata = buffersInFlight.remove(ref);

        if (metadata == null)
            throw new IllegalArgumentException("returned buffer " + ref.hashCode + " was never allocated by this pool");
        if (metadata.sizeBytes != justReleased.capacity()) {
            throw new IllegalStateException("buffer " + ref.hashCode + " has capacity " + justReleased.capacity()
                    + " but recorded as " + metadata.sizeBytes);
        }

        log.trace("released buffer of size {} and identity {}", metadata.sizeBytes, ref.hashCode);
    }

    @Override
    public void close() {
        alive = false;
        gcListenerThread.interrupt();
    }

    private class GarbageCollectionListener implements Runnable {
        @Override
        public void run() {
            while (alive) {
                try {
                    BufferReference ref = (BufferReference) garbageCollectedBuffers.remove();
                    ref.clear();

                    BufferMetadata metadata = buffersInFlight.remove(ref);

                    if (metadata == null) {
                        continue;
                    }

                    availableMemory.addAndGet(metadata.sizeBytes);
                    log.error(
                            "Reclaimed buffer of size {} and identity {} that was not properly released. This is a bug.",
                            metadata.sizeBytes, ref.hashCode);
                } catch (InterruptedException e) {
                    log.debug("interrupted", e);
                }
            }
            log.info("GC listener shutting down");
        }
    }

    private static final class BufferMetadata {
        private final int sizeBytes;

        private BufferMetadata(int sizeBytes) {
            this.sizeBytes = sizeBytes;
        }
    }

    private static final class BufferReference extends WeakReference<ByteBuffer> {
        private final int hashCode;

        private BufferReference(ByteBuffer referent) {
            this(referent, null);
        }

        private BufferReference(ByteBuffer referent, ReferenceQueue<? super ByteBuffer> q) {
            super(referent, q);
            hashCode = System.identityHashCode(referent);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BufferReference that = (BufferReference) o;
            if (hashCode != that.hashCode) {
                return false;
            }

            ByteBuffer thisBuf = get();
            if (thisBuf == null) {
                return false;
            }

            ByteBuffer thatBuf = that.get();
            return thisBuf == thatBuf;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "GarbageCollectedMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes)
                + " used in " + buffersInFlight.size() + " buffers}";
    }
}
