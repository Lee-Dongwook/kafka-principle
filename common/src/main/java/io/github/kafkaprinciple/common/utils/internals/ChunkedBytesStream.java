package io.github.kafkaprinciple.common.utils.internals;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ChunkedBytesStream extends FilterInputStream {
    private final BufferSupplier bufferSupplier;

    private byte[] intermediateBuf;

    protected int count = 0;

    protected int pos = 0;

    private final ByteBuffer intermediateBufRef;

    private final boolean delegateSkipToSourceStream;

    public ChunkedBytesStream(InputStream in, BufferSupplier bufferSupplier, int intermediateBufSize,
            boolean delegateSkipToSourceStream) {
        super(in);
        this.bufferSupplier = bufferSupplier;
        intermediateBufRef = bufferSupplier.get(intermediateBufSize);
        if (!intermediateBufRef.hasArray() || (intermediateBufRef.arrayOffset() != 0)) {
            throw new IllegalArgumentException("provided ByteBuffer lacks array or has non-zero arrayOffset");
        }
        intermediateBuf = intermediateBufRef.array();
        this.delegateSkipToSourceStream = delegateSkipToSourceStream;
    }

    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = intermediateBuf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    @Override
    public int read() throws IOException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                return -1;
        }

        return getBufIfOpen()[pos++] & 0xff;
    }

    InputStream getInIfOpen() throws IOException {
        InputStream input = in;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }

    int fill() throws IOException {
        byte[] buffer = getBufIfOpen();
        pos = 0;
        count = pos;
        int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
        if (n > 0)
            count = n + pos;
        return n;
    }

    @Override
    public void close() throws IOException {
        byte[] mybuf = intermediateBuf;
        intermediateBuf = null;

        InputStream input = in;
        in = null;

        if (mybuf != null)
            bufferSupplier.release(intermediateBufRef);
        if (input != null)
            input.close();
    }

    public int read(byte[] b, int off, int len) throws IOException {
        getBufIfOpen();
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (;;) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0)
                return (n == 0) ? nread : n;
            n += nread;
            if (n >= len)
                return n;
            InputStream input = in;
            if (input != null && input.available() <= 0)
                return n;
        }
    }

    private int read1(byte[] b, int off, int len) throws IOException {
        int avail = count - pos;
        if (avail <= 0) {
            if (len >= getBufIfOpen().length) {
                return getInIfOpen().read(b, off, len);
            }
            fill();
            avail = count - pos;
            if (avail <= 0)
                return -1;
        }
        int cnt = Math.min(avail, len);
        System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
        pos += cnt;
        return cnt;
    }

    @Override
    public long skip(long toSkip) throws IOException {
        getBufIfOpen();
        if (toSkip <= 0) {
            return 0;
        }

        long remaining = toSkip;

        int avail = count - pos;
        int bytesSkipped = (int) Math.min(avail, remaining);
        pos += bytesSkipped;
        remaining -= bytesSkipped;

        while (remaining > 0) {
            if (delegateSkipToSourceStream) {
                long delegateBytesSkipped = getInIfOpen().skip(remaining);

                if (delegateBytesSkipped == 0) {
                    if (read() == -1) {
                        break;
                    }

                    remaining--;
                } else if (delegateBytesSkipped > remaining || delegateBytesSkipped < 0) {
                    throw new IOException("Unable to skip exactly");
                }

                remaining -= delegateBytesSkipped;
            } else {
                if (pos >= count) {
                    fill();

                    if (pos >= count) {
                        break;
                    }
                }

                avail = count - pos;
                bytesSkipped = (int) Math.min(avail, remaining);
                pos += bytesSkipped;
                remaining -= bytesSkipped;
            }
        }
        return toSkip - remaining;
    }

    public InputStream sourceStream() {
        return in;
    }

    @Override
    public synchronized int available() throws IOException {
        int n = count - pos;
        int avail = getInIfOpen().available();
        return n > (Integer.MAX_VALUE - avail)
                ? Integer.MAX_VALUE
                : n + avail;
    }
}
