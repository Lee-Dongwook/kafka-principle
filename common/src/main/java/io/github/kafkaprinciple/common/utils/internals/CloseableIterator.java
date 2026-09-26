package io.github.kafkaprinciple.common.utils.internals;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {
    void close();

    static <R> CloseableIterator<R> wrap(Iterator<R> inner) {
        return new CloseableIterator<>() {
            @Override 
            public void close() {
            }
            
            @Override 
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override 
            public R next() {
                return inner.next();
            }

            @Override 
            public void remove() {
                inner.remove();
            }
        };
    }
}
