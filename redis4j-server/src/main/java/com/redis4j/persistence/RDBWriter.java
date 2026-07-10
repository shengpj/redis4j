package com.redis4j.persistence;

import com.redis4j.storage.DataType;
import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import com.redis4j.storage.snapshot.SnapshotProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Writes the Redis4J snapshot format using a stable DataSnapshot. */
public class RDBWriter {
    private static final Logger logger = LoggerFactory.getLogger(RDBWriter.class);
    private static final byte[] RDB_HEADER = "REDIS0011".getBytes(StandardCharsets.US_ASCII);
    private static final int BUFFER_SIZE = 64 * 1024;

    private FileChannel channel;
    private ByteBuffer buffer;
    private final Map<DataType, SnapshotValueWriter> codecs = createCodecs();

    public void save(SnapshotProvider snapshotProvider, String filePath) throws IOException {
        logger.info("Saving RDB file to {}", filePath);
        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Unable to create data directory: " + parent);
        }

        long start = System.currentTimeMillis();
        DataSnapshot snapshot = snapshotProvider.createSnapshot();
        try (FileChannel output = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel = output;
            buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            writeHeader();
            writeEntries(snapshot);
            writeFooter();
            flushBuffer();
            channel.force(true);
        }
        logger.info("RDB file saved successfully in {} ms: {}", System.currentTimeMillis() - start, filePath);
    }

    private void writeEntries(DataSnapshot snapshot) throws IOException {
        int count = 0;
        for (SnapshotEntry entry : snapshot.entries()) {
            SnapshotValueWriter codec = codecs.get(entry.type());
            if (codec == null || entry.expireAt() > 0 && entry.expireAt() <= System.currentTimeMillis()) continue;
            ensure(512);
            buffer.put((byte) 0xC0);
            writeString(entry.key());
            boolean expires = entry.expireAt() > 0;
            buffer.put(expires ? (byte) 1 : (byte) 0);
            if (expires) writeInt64(entry.expireAt());
            codec.write(entry);
            count++;
        }
        logger.debug("Wrote {} key-value pairs to RDB", count);
    }

    private Map<DataType, SnapshotValueWriter> createCodecs() {
        Map<DataType, SnapshotValueWriter> result = new EnumMap<>(DataType.class);
        result.put(DataType.STRING, entry -> {
            buffer.put((byte) 0x00);
            writeString((String) entry.value());
        });
        result.put(DataType.LIST, entry -> {
            buffer.put((byte) 0x01);
            List<String> values = cast(entry.value());
            writeInt32(values.size());
            for (String value : values) writeString(value);
        });
        result.put(DataType.SET, entry -> {
            buffer.put((byte) 0x02);
            Set<String> values = cast(entry.value());
            writeInt32(values.size());
            for (String value : values) writeString(value);
        });
        result.put(DataType.HASH, entry -> {
            buffer.put((byte) 0x04);
            Map<String, String> values = cast(entry.value());
            writeInt32(values.size());
            for (Map.Entry<String, String> value : values.entrySet()) {
                writeString(value.getKey());
                writeString(value.getValue());
            }
        });
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) { return (T) value; }

    private void writeHeader() throws IOException {
        ensure(18);
        buffer.put(RDB_HEADER);
        buffer.putInt(9);
        buffer.put((byte) 0xFE);
        buffer.putInt(0);
    }

    private void writeFooter() throws IOException {
        ensure(5);
        buffer.put((byte) 0xFF);
        buffer.putInt(0);
    }

    private void writeString(String value) throws IOException {
        if (value == null) {
            writeInt32(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeInt32(bytes.length);
        if (bytes.length > buffer.capacity()) {
            flushBuffer();
            ByteBuffer largeValue = ByteBuffer.wrap(bytes);
            while (largeValue.hasRemaining()) channel.write(largeValue);
        } else {
            ensure(bytes.length);
            buffer.put(bytes);
        }
    }

    private void writeInt32(int value) throws IOException { ensure(4); buffer.putInt(value); }
    private void writeInt64(long value) throws IOException { ensure(8); buffer.putLong(value); }

    private void ensure(int bytes) throws IOException {
        if (bytes > buffer.capacity()) throw new IOException("RDB value exceeds write buffer");
        if (buffer.remaining() < bytes) flushBuffer();
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) channel.write(buffer);
        buffer.clear();
    }

    @FunctionalInterface
    private interface SnapshotValueWriter {
        void write(SnapshotEntry entry) throws IOException;
    }
}
