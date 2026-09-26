package io.github.kafkaprinciple.common.utils;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.config.ConfigDef;
import io.github.kafkaprinciple.common.config.ConfigException;
import io.github.kafkaprinciple.common.network.TransferableChannel;
import io.github.kafkaprinciple.common.utils.internals.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public final class Utils {
    private Utils() {
    }

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(?:[0-9a-zA-Z\\-%._]*://)?\\[?([0-9a-zA-Z\\-%._:]*)]?:([0-9]+)");
    private static final Pattern VALID_HOST_CHARACTERS = Pattern.compile("([0-9a-zA-Z\\-%._:]*)");

    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##",
            DecimalFormatSymbols.getInstance(Locale.ENGLISH));
    
    private static final String[] BYTE_SCALE_SUFFIXES = new String[] { "B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB",
            "YB" };
    
    public static final String NL = System.lineSeparator();

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private static final VarHandle INT_HANDLE =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    public static <T extends Comparable<? super T>> List<T> sorted(Collection<T> collection) {
        List<T> res = new ArrayList<>(collection);
        Collections.sort(res);
        return Collections.unmodifiableList(res);
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String utf8(ByteBuffer buffer, int length) {
        return utf8(buffer, 0, length);
    }

    public static String utf8(ByteBuffer buffer) {
        return utf8(buffer, buffer.remaining());
    }

    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray())
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length,
                    StandardCharsets.UTF_8);
        else
            return utf8(toArray(buffer, offset, length));
    }
    
    public static byte[] utf8(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    public static long min(long first, long... rest) {
        long min = first;
        for (long r : rest) {
            if (r < min)
                min = r;
        }
        return min;
    }

    public static long max(long first, long... rest) {
        long max = first;
        for (long r : rest) {
            if (r > max)
                max = r;
        }
        return max;
    }

    public static short min(short first, short second) {
        return (short) Math.min(first, second);
    }

    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }
    
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    public static byte[] toArray(ByteBuffer buffer, int size) {
        return toArray(buffer, 0, size);
    }

    public static byte[] toNullableArray(ByteBuffer buffer) {
        return buffer == null ? null : toArray(buffer);
    }

    public static ByteBuffer wrapNullable(byte[] array) {
        return array == null ? null : ByteBuffer.wrap(array);
    }

    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    public static byte[] getNullableSizePrefixedArray(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        return getNullableArray(buffer, size);
    }

    public static byte[] getNullableArray(final ByteBuffer buffer, final int size) {
        if (size > buffer.remaining()) {
            throw new BufferUnderflowException();
        }
        final byte[] oldBytes = size == -1 ? null : new byte[size];
        if (oldBytes != null) {
            buffer.get(oldBytes);
        }
        return oldBytes;
    }

    public static byte[] copyArray(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }

    public static boolean isEqualConstantTime(char[] first, char[] second) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }

        if (second.length == 0) {
            return first.length == 0;
        }

        boolean matches = first.length == second.length;
        for (int i = 0; i < first.length; ++i) {
            int j = i < second.length ? i : 0;
            if (first[i] != second[j]) {
                matches = false;
            }
        }
        return matches;
    }
    
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T newInstance(Class<T> c) {
        if (c == null)
            throw new KafkaException("class cannot be null");
        try {
            return c.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new KafkaException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        }
    }

    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        return Utils.newInstance(loadClass(klass, base));
    }

    public static <T> Class<? extends T> loadClass(String klass, Class<T> base) throws ClassNotFoundException {
        ClassLoader contextOrKafkaClassLoader = Utils.getContextOrKafkaClassLoader();
        Class<?> loadedClass = contextOrKafkaClassLoader.loadClass(klass);
        return Class.forName(loadedClass.getName(), true, contextOrKafkaClassLoader).asSubclass(base);
    }

    public static <T> T newInstance(Class<?> klass, Class<T> base) {
        return Utils.newInstance(klass.asSubclass(base));
    }

    public static <T> T newParameterizedInstance(String className, Object... params)
            throws ClassNotFoundException {
        Class<?>[] argTypes = new Class<?>[params.length / 2];
        Object[] args = new Object[params.length / 2];
        try {
            Class<?> c = Utils.loadClass(className, Object.class);
            for (int i = 0; i < params.length / 2; i++) {
                argTypes[i] = (Class<?>) params[2 * i];
                args[i] = params[(2 * i) + 1];
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor = (Constructor<T>) c.getConstructor(argTypes);
            return constructor.newInstance(args);
        } catch (NoSuchMethodException e) {
            throw new ClassNotFoundException(String.format("Failed to find " +
                    "constructor with %s for %s",
                    Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(", ")), className), e);
        } catch (InstantiationException e) {
            throw new ClassNotFoundException(String.format("Failed to instantiate " +
                    "%s", className), e);
        } catch (IllegalAccessException e) {
            throw new ClassNotFoundException(String.format("Unable to access " +
                    "constructor of %s", className), e);
        } catch (InvocationTargetException e) {
            throw new KafkaException(String.format("The constructor of %s threw an exception", className),
                    e.getCause());
        }
    }
    
    @SuppressWarnings("fallthrough")
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        int length4 = length >> 2;

        for (int i = 0; i < length4; i++) {
            final int i4 = i << 2;
            int k = (int) INT_HANDLE.get(data, i4);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int index = length4 << 2;
        switch (length - index) {
            case 3:
                h ^= (data[index + 2] & 0xff) << 16;
            case 2:
                h ^= (data[index + 1] & 0xff) << 8;
            case 1:
                h ^= data[index] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    public static boolean validHostPattern(String address) {
        return VALID_HOST_CHARACTERS.matcher(address).matches();
    }

    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                ? "[" + host + "]:" + port
                : host + ":" + port;
    }

    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return String.valueOf(bytes);
        }
        if (bytes == 0) {
            return "0 " + BYTE_SCALE_SUFFIXES[0];
        }
        double asDouble = (double) bytes;
        int ordinal = (int) Math.floor(Math.log(asDouble) / Math.log(1024.0));
        double scale = Math.pow(1024.0, ordinal);
        double scaled = asDouble / scale;
        String formatted = TWO_DIGIT_FORMAT.format(scaled);
        try {
            return formatted + " " + BYTE_SCALE_SUFFIXES[ordinal];
        } catch (IndexOutOfBoundsException e) {
            //huge number?
            return String.valueOf(asDouble);
        }
    }

    public static <K, V> String mkString(Map<K, V> map, String begin, String end,
                                         String keyValueSeparator, String elementSeparator) {
        StringBuilder bld = new StringBuilder();
        bld.append(begin);
        String prefix = "";
        for (Map.Entry<K, V> entry : map.entrySet()) {
            bld.append(prefix).append(entry.getKey()).
                    append(keyValueSeparator).append(entry.getValue());
            prefix = elementSeparator;
        }
        bld.append(end);
        return bld.toString();
    }

    public static Map<String, String> parseMap(String mapStr, String keyValueSeparator, String elementSeparator) {
        Map<String, String> map = new HashMap<>();

        if (!mapStr.isEmpty()) {
            String[] attrvals = mapStr.split(elementSeparator);
            for (String attrval : attrvals) {
                String[] array = attrval.split(keyValueSeparator, 2);
                map.put(array[0], array[1]);
            }
        }
        return map;
    }

    public static Properties loadProps(String filename) throws IOException {
        return loadProps(filename, null);
    }
    
    public static Properties loadProps(String filename, List<String> onlyIncludeKeys) throws IOException {
        Properties props = new Properties();

        if (filename != null) {
            try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
                props.load(propStream);
            }
        } else {
            System.out.println("Did not load any properties since the property file is not specified");
        }

        if (onlyIncludeKeys == null || onlyIncludeKeys.isEmpty())
            return props;
        Properties requestedProps = new Properties();
        onlyIncludeKeys.forEach(key -> {
            String value = props.getProperty(key);
            if (value != null)
                requestedProps.setProperty(key, value);
        });
        return requestedProps;
    }


    public static Map<String, String> propsToStringMap(Properties props) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet())
            result.put(entry.getKey().toString(), entry.getValue().toString());
        return result;
    }

    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }

    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    public static ByteBuffer readBytes(ByteBuffer srcBuf, int bytesToRead) {
        if (bytesToRead < 0)
            return null;

        final ByteBuffer dstBuf = srcBuf.slice();
        dstBuf.limit(bytesToRead);
        srcBuf.position(srcBuf.position() + bytesToRead);

        return dstBuf;
    }


    public static String readFileAsString(String path) throws IOException {
        try {
            byte[] allBytes = Files.readAllBytes(Paths.get(path));
            return new String(allBytes, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IOException("Unable to read file " + path, ex);
        }
    }

    public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
        if (newLength > existingBuffer.capacity()) {
            ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
            existingBuffer.flip();
            newBuffer.put(existingBuffer);
            return newBuffer;
        }
        return existingBuffer;
    }

    @SafeVarargs
    public static <T extends Comparable<T>> SortedSet<T> mkSortedSet(T... elems) {
        SortedSet<T> result = new TreeSet<>();
        for (T elem : elems)
            result.add(elem);
        return result;
    }

    public static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
        return new AbstractMap.SimpleEntry<>(k, v);
    }


    @SafeVarargs
    public static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
        final LinkedHashMap<K, V> result = new LinkedHashMap<>();
        for (final Map.Entry<K, V> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static Properties mkProperties(final Map<String, String> properties) {
        final Properties result = new Properties();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static Properties mkObjectProperties(final Map<String, Object> properties) {
        final Properties result = new Properties();
        for (final Map.Entry<String, Object> entry : properties.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }


    public static void delete(final File rootFile) throws IOException {
        if (rootFile == null)
            return;
        Files.walkFileTree(rootFile.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                if (exc instanceof NoSuchFileException) {
                    if (path.toFile().equals(rootFile)) {
                        return FileVisitResult.TERMINATE;
                    } else {
                        return FileVisitResult.CONTINUE;
                    }
                }
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }

                Files.deleteIfExists(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }


    public static ClassLoader getContextOrKafkaClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            return Utils.class.getClassLoader();
        else
            return cl;
    }

    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        atomicMoveWithFallback(source, target, true);
    }

    public static void atomicMoveWithFallback(Path source, Path target, boolean needFlushParentDir) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                log.warn("Failed atomic move of {} to {} retrying with a non-atomic move", source, target, outer);
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Non-atomic move of {} to {} succeeded after atomic move failed", source, target);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        } finally {
            if (needFlushParentDir) {
                flushDir(target.toAbsolutePath().normalize().getParent());
            }
        }
    }

    public static void flushDir(Path path) throws IOException {
        if (path != null && !OperatingSystem.IS_WINDOWS && !OperatingSystem.IS_ZOS) {
            try (FileChannel dir = FileChannel.open(path, StandardOpenOption.READ)) {
                dir.force(true);
            }
        }
    }

    public static void flushDirIfExists(Path path) throws IOException {
        try {
            flushDir(path);
        } catch (NoSuchFileException e) {
            log.warn("Failed to flush directory {}", path);
        }
    }


    public static void flushFileIfExists(Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            fileChannel.force(true);
        } catch (NoSuchFileException e) {
            log.warn("Failed to flush file {}", path, e);
        }
    }

    public static void closeAll(Closeable... closeables) throws IOException {
        IOException exception = null;
        for (Closeable closeable : closeables) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (IOException e) {
                if (exception != null)
                    exception.addSuppressed(e);
                else
                    exception = e;
            }
        }
        if (exception != null)
            throw exception;
    }

    @FunctionalInterface
    public interface SwallowAction {
        void run() throws Throwable;
    }

    public static void swallow(final SwallowAction code) {
        swallow(log, Level.WARN, "Exception while execution the action", code, null);
    }

    public static void swallow(final Logger log, final SwallowAction code) {
        swallow(log, Level.WARN, "Exception while execution the action", code, null);
    }

    public static void swallow(final Logger log, final Level level, final SwallowAction code) {
        swallow(log, level, "Exception while execution the action", code, null);
    }

    public static void swallow(final Logger log, final Level level, final String errorMessage, final SwallowAction code) {
        swallow(log, level, errorMessage, code, null);
    }

    public static void swallow(final Logger log, final Level level, final String errorMessage, final SwallowAction code,
                               final AtomicReference<Throwable> firstException) {
        if (code != null) {
            try {
                code.run();
            } catch (Throwable t) {
                switch (level) {
                    case INFO:
                        log.info(errorMessage, t);
                        break;
                    case DEBUG:
                        log.debug(errorMessage, t);
                        break;
                    case ERROR:
                        log.error(errorMessage, t);
                        break;
                    case TRACE:
                        log.trace(errorMessage, t);
                        break;
                    case WARN:
                    default:
                        log.warn(errorMessage, t);
                }
                if (firstException != null)
                    firstException.compareAndSet(null, t);
            }
        }
    }

    @FunctionalInterface
    public interface UncheckedCloseable extends AutoCloseable {
        @Override
        void close();
    }

    public static void maybeCloseQuietly(Object maybeCloseable, String name) {
        if (maybeCloseable instanceof AutoCloseable)
            closeQuietly((AutoCloseable) maybeCloseable, name);
    }

    public static void closeQuietly(AutoCloseable closeable, String name) {
        closeQuietly(closeable, name, log);
    }

    public static void closeQuietly(AutoCloseable closeable, String name, Logger logger) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                logger.warn("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    public static void closeQuietly(AutoCloseable closeable, String name, AtomicReference<Throwable> firstException) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    public static void closeAllQuietly(AtomicReference<Throwable> firstException, String name, AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) closeQuietly(closeable, name, firstException);
    }


    public static void tryAll(List<Callable<Void>> all) throws Throwable {
        Throwable exception = null;
        for (Callable<Void> call : all) {
            try {
                call.call();
            } catch (Throwable t) {
                if (exception != null)
                    exception.addSuppressed(t);
                else
                    exception = t;
            }
        }
        if (exception != null)
            throw exception;
    }

    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }


    public static ByteBuffer sizeDelimited(ByteBuffer buffer, int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }


    public static void readFullyOrFail(FileChannel channel, ByteBuffer destinationBuffer, long position,
                                       String description) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                    "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    public static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    public static int readFully(InputStream inputStream, ByteBuffer destinationBuffer) throws IOException {
        if (!destinationBuffer.hasArray())
            throw new IllegalArgumentException("destinationBuffer must be backed by an array");
        int initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position();
        byte[] array = destinationBuffer.array();
        int length = destinationBuffer.remaining();
        int totalBytesRead = 0;
        do {
            int bytesRead = inputStream.read(array, initialOffset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1)
                break;
            totalBytesRead += bytesRead;
        } while (length > totalBytesRead);
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead);
        return totalBytesRead;
    }

    public static void writeFully(FileChannel channel, ByteBuffer sourceBuffer) throws IOException {
        while (sourceBuffer.hasRemaining())
            channel.write(sourceBuffer);
    }

    public static int tryWriteTo(TransferableChannel destChannel,
                                  int position,
                                  int length,
                                  ByteBuffer sourceBuffer) throws IOException {

        ByteBuffer dup = sourceBuffer.duplicate();
        dup.position(position);
        dup.limit(position + length);
        return destChannel.write(dup);
    }

    public static void writeTo(DataOutput out, ByteBuffer buffer, int length) throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.position() + buffer.arrayOffset(), length);
        } else {
            int pos = buffer.position();
            for (int i = pos; i < length + pos; i++)
                out.writeByte(buffer.get(i));
        }
    }

    public static <T> List<T> toList(Iterable<T> iterable) {
        return toList(iterable.iterator());
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext())
            res.add(iterator.next());
        return res;
    }

    public static <T> List<T> toList(Iterator<T> iterator, Predicate<T> predicate) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext()) {
            T e = iterator.next();
            if (predicate.test(e)) {
                res.add(e);
            }
        }
        return res;
    }

    public static int to32BitField(final Set<Byte> bytes) {
        int value = 0;
        for (final byte b : bytes)
            value |= 1 << checkRange(b);
        return value;
    }

    private static byte checkRange(final byte i) {
        if (i > 31)
            throw new IllegalArgumentException("out of range: i>31, i = " + i);
        if (i < 0)
            throw new IllegalArgumentException("out of range: i<0, i = " + i);
        return i;
    }

    public static Set<Byte> from32BitField(final int intValue) {
        Set<Byte> result = new HashSet<>();
        for (int itr = intValue, count = 0; itr != 0; itr >>>= 1) {
            if ((itr & 1) != 0)
                result.add((byte) count);
            count++;
        }
        return result;
    }

    public static <K, V, M extends Map<K, V>> Collector<Map.Entry<K, V>, M, M> entriesToMap(final Supplier<M> mapSupplier) {
        return new Collector<>() {
            @Override
            public Supplier<M> supplier() {
                return mapSupplier;
            }

            @Override
            public BiConsumer<M, Map.Entry<K, V>> accumulator() {
                return (map, entry) -> map.put(entry.getKey(), entry.getValue());
            }

            @Override
            public BinaryOperator<M> combiner() {
                return (map, map2) -> {
                    map.putAll(map2);
                    return map;
                };
            }

            @Override
            public Function<M, M> finisher() {
                return map -> map;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH);
            }
        };
    }

    @SafeVarargs
    public static <E> Set<E> union(final Supplier<Set<E>> constructor, final Set<E>... set) {
        final Set<E> result = constructor.get();
        for (final Set<E> s : set) {
            result.addAll(s);
        }
        return result;
    }

    @SafeVarargs
    public static <E> Set<E> intersection(final Supplier<Set<E>> constructor, final Set<E> first, final Set<E>... set) {
        final Set<E> result = constructor.get();
        result.addAll(first);
        for (final Set<E> s : set) {
            result.retainAll(s);
        }
        return result;
    }

    public static <E> Set<E> diff(final Supplier<Set<E>> constructor, final Set<E> left, final Set<E> right) {
        final Set<E> result = constructor.get();
        result.addAll(left);
        result.removeAll(right);
        return result;
    }

    public static <K, V> Map<K, V> filterMap(final Map<K, V> map, final Predicate<Entry<K, V>> filterPredicate) {
        return map.entrySet().stream().filter(filterPredicate).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public static Map<String, Object> propsToMap(Properties properties) {
        Enumeration<?> enumeration;
        try {
            enumeration = properties.propertyNames();
        } catch (ClassCastException e) {
            throw new ConfigException("One or more keys is not a string.");
        }
        Map<String, Object> map = new HashMap<>();
        while (enumeration.hasMoreElements()) {
            String key = (String) enumeration.nextElement();
            Object value = (properties.get(key) != null) ? properties.get(key) : properties.getProperty(key);
            map.put(key, value);
        }
        return map;
    }

    public static Map<String, Object> castToStringObjectMap(Map<?, ?> inputMap) {
        if (inputMap instanceof Properties) {
            return propsToMap((Properties) inputMap);
        }
        Map<String, Object> map = new HashMap<>(inputMap.size());
        for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, entry.getValue());
            } else {
                throw new ConfigException(String.valueOf(entry.getKey()), entry.getValue(), "Key must be a string.");
            }
        }
        return map;
    }


    public static long getDateTime(String timestamp) throws ParseException, IllegalArgumentException {
        if (timestamp == null) {
            throw new IllegalArgumentException("Error parsing timestamp with null value");
        }

        final String[] timestampParts = timestamp.split("T");
        if (timestampParts.length < 2) {
            throw new ParseException("Error parsing timestamp. It does not contain a 'T' according to ISO8601 format", timestamp.length());
        }

        final String secondPart = timestampParts[1];
        if (!(secondPart.contains("+") || secondPart.contains("-") || secondPart.contains("Z"))) {
            timestamp = timestamp + "Z";
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
        simpleDateFormat.setLenient(false);
        try {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        } catch (final ParseException e) {
            simpleDateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
            final Date date = simpleDateFormat.parse(timestamp);
            return date.getTime();
        }
    }

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static String[] enumOptions(Class<? extends Enum<?>> enumClass) {
        Objects.requireNonNull(enumClass);
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException("Class " + enumClass + " is not an enumerable type");
        }

        return Stream.of(enumClass.getEnumConstants())
                .map(Object::toString)
                .toArray(String[]::new);
    }

    public static void ensureConcreteSubclass(Class<?> baseClass, Class<?> klass) {
        Objects.requireNonNull(baseClass);
        Objects.requireNonNull(klass);

        if (!baseClass.isAssignableFrom(klass)) {
            String inheritFrom = baseClass.isInterface() ? "implement" : "extend";
            String baseClassType = baseClass.isInterface() ? "interface" : "class";
            throw new ConfigException("Class " + klass + " does not " + inheritFrom + " the " + baseClass.getSimpleName() + " " + baseClassType);
        }

        if (Modifier.isAbstract(klass.getModifiers())) {
            String childClassNames = Stream.of(klass.getClasses())
                    .filter(baseClass::isAssignableFrom)
                    .filter(c -> !Modifier.isAbstract(c.getModifiers()))
                    .filter(c -> Modifier.isPublic(c.getModifiers()))
                    .map(Class::getName)
                    .collect(Collectors.joining(", "));
            String message = "This class is abstract and cannot be created.";
            if (!Utils.isBlank(childClassNames))
                message += " Did you mean " + childClassNames + "?";
            throw new ConfigException(message);
        }
    }

    public static String toLogDateTimeFormat(long timestamp) {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS XXX");
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(dateTimeFormatter);
    }

    public static String replaceSuffix(String str, String oldSuffix, String newSuffix) {
        if (!str.endsWith(oldSuffix))
            throw new IllegalArgumentException("Expected string to end with " + oldSuffix + " but string is " + str);
        return str.substring(0, str.length() - oldSuffix.length()) + newSuffix;
    }

    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix) {
        return entriesWithPrefix(map, prefix, true);
    }

    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip) {
        return entriesWithPrefix(map, prefix, strip, false);
    }

    public static <V> Map<String, V> entriesWithPrefix(Map<String, V> map, String prefix, boolean strip, boolean allowMatchingLength) {
        Map<String, V> result = new HashMap<>();
        for (Map.Entry<String, V> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix) && (allowMatchingLength || entry.getKey().length() > prefix.length())) {
                if (strip)
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public static void require(boolean requirement) {
        if (!requirement)
            throw new IllegalArgumentException("requirement failed");
    }

    public static void require(boolean requirement, String errorMessage) {
        if (!requirement)
            throw new IllegalArgumentException(errorMessage);
    }


    public static ConfigDef mergeConfigs(List<ConfigDef> configDefs) {
        ConfigDef all = new ConfigDef();
        configDefs.forEach(configDef -> configDef.configKeys().values().forEach(all::define));
        return all;
    }

    public static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
                mbs.registerMBean(mbean, objName);
                return true;
            }
        } catch (Exception e) {
            log.error("Failed to register Mbean with name {}", name, e);
            return false;
        }
    }


    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
    
    public static long msToNs(long timeMs) {
        try {
            return Math.multiplyExact(1000 * 1000, timeMs);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Cannot convert " + timeMs + " millisecond to nanosecond due to arithmetic overflow", e);
        }
    }
}
