package io.github.kafkaprinciple.common.compress;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class GzipOutputStream extends GZIPOutputStream {
    public GzipOutputStream(OutputStream out, int size, int level) throws IOException {
        super(out, size);
        setLevel(level);
    }

    private void setLevel(int level) {
        def.setLevel(level);
    }
}
