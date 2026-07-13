package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis4J 快照文件读取器。
 *
 * <p>采用“先完整解析并校验、再写入 DataStore”的两阶段流程，避免损坏文件
 * 在解析到一半时向数据库恢复部分数据。</p>
 */
public class RDBReader {
    private static final Logger logger = LoggerFactory.getLogger(RDBReader.class);
    private static final byte[] RDB_HEADER = "REDIS0011".getBytes(StandardCharsets.US_ASCII);
    private static final long MAX_RDB_SIZE = 1024L * 1024 * 1024;
    private static final int MAX_KEY_LENGTH = 10_000_000;
    private static final int MAX_VALUE_LENGTH = 64 * 1024 * 1024;
    private static final int MAX_COLLECTION_ENTRIES = 1_000_000;

    private long lastSaveTimestamp;
    // 文件中的类型编码映射到对应的反序列化策略。
    private final Map<Integer, ValueReader> codecs = Map.of(
            0x00, buffer -> readString(buffer, MAX_VALUE_LENGTH, "string value"),
            0x01, this::readList,
            0x02, this::readSet,
            0x03, this::readZSet,
            0x04, this::readHash);

    public void load(DataStore dataStore, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.info("RDB file {} does not exist, skipping", filePath);
            return;
        }
        long fileSize = file.length();
        // 映射文件前先限制整体大小，避免异常文件占用过多虚拟内存和堆内存。
        if (fileSize <= 0 || fileSize > MAX_RDB_SIZE) {
            throw new IOException("Invalid RDB file size: " + fileSize);
        }

        logger.info("Loading RDB file from {}", filePath);
        List<LoadedEntry> entries;
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            // 只读内存映射适合顺序解析，避免对每个字段执行独立的文件读取调用。
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            // parse 阶段不修改 DataStore，只有整个文件校验成功后才会进入 apply 阶段。
            entries = parse(buffer);
        } catch (RuntimeException e) {
            throw new IOException("RDB file is truncated or corrupted", e);
        }

        // 文件结构完整后统一恢复，防止截断文件造成半恢复状态。
        int loaded = apply(entries, dataStore);
        lastSaveTimestamp = file.lastModified() / 1000;
        logger.info("RDB file loaded successfully: {} keys restored", loaded);
    }

    /** 从混合持久化文件的指定区间恢复一份完整 RDB，且不对源文件建立内存映射。 */
    public void loadRegion(DataStore dataStore, Path path, long offset, long length) throws IOException {
        if (offset < 0 || length <= 0 || length > MAX_RDB_SIZE || length > Integer.MAX_VALUE) {
            throw new IOException("Invalid RDB preamble region: offset=" + offset + ", length=" + length);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) length);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read < 0) throw new IOException("Truncated RDB preamble");
                if (read == 0) throw new IOException("Unable to make progress while reading RDB preamble");
            }
        }
        buffer.flip();
        List<LoadedEntry> entries;
        try {
            entries = parse(buffer);
        } catch (RuntimeException e) {
            throw new IOException("RDB preamble is truncated or corrupted", e);
        }
        int loaded = apply(entries, dataStore);
        lastSaveTimestamp = Files.getLastModifiedTime(path).toMillis() / 1000;
        logger.info("RDB preamble loaded successfully: {} keys restored", loaded);
    }

    private List<LoadedEntry> parse(ByteBuffer buffer) throws IOException {
        // 魔数和版本必须精确匹配，避免用错误格式解释后续字节。
        requireRemaining(buffer, RDB_HEADER.length + Integer.BYTES, "header");
        byte[] header = new byte[RDB_HEADER.length];
        buffer.get(header);
        if (!Arrays.equals(header, RDB_HEADER)) throw new IOException("Invalid RDB file header");
        int version = buffer.getInt();
        if (version != 9) throw new IOException("Unsupported RDB format version: " + version);

        List<LoadedEntry> entries = new ArrayList<>();
        boolean footerSeen = false;
        while (buffer.hasRemaining()) {
            int marker = readUnsignedByte(buffer, "entry marker");
            if (marker == 0xFF) {
                // Footer 后不允许存在额外字节，确保文件边界明确且没有拼接脏数据。
                requireRemaining(buffer, Integer.BYTES, "footer");
                if (buffer.getInt() != 0) throw new IOException("Unsupported RDB checksum");
                if (buffer.hasRemaining()) throw new IOException("Trailing bytes after RDB footer");
                footerSeen = true;
                break;
            }
            if (marker == 0xFE) {
                // 0xFE 后跟数据库编号；当前仅使用 DB 0，但仍消费该字段以保持格式可扩展。
                requireRemaining(buffer, Integer.BYTES, "database number");
                buffer.getInt();
                continue;
            }
            if (marker != 0xC0) throw new IOException("Unknown RDB entry marker: 0x" + Integer.toHexString(marker));

            String key = readString(buffer, MAX_KEY_LENGTH, "key");
            if (key == null) throw new IOException("RDB key cannot be null");
            int expireFlag = readUnsignedByte(buffer, "expiry flag");
            if (expireFlag != 0 && expireFlag != 1) throw new IOException("Invalid expiry flag: " + expireFlag);
            long expireAt = expireFlag == 1 ? readLong(buffer, "expiry timestamp") : -1;
            int typeCode = readUnsignedByte(buffer, "value type");
            ValueReader reader = codecs.get(typeCode);
            if (reader == null) throw new IOException("Unknown RDB value type: 0x" + Integer.toHexString(typeCode));
            Object value = reader.read(buffer);
            // 此处只保存解析结果，不立即调用 DataStore。
            entries.add(new LoadedEntry(key, type(typeCode), value, expireAt));
        }
        if (!footerSeen) throw new IOException("RDB footer is missing");
        return entries;
    }

    private int apply(List<LoadedEntry> entries, DataStore store) {
        int count = 0;
        for (LoadedEntry entry : entries) {
            // 从保存到恢复之间可能已经过期，因此恢复前需要根据当前时间再次判断。
            long remaining = entry.expireAt() > 0 ? entry.expireAt() - System.currentTimeMillis() : -1;
            if (entry.expireAt() > 0 && remaining <= 0) continue;
            switch (entry.type()) {
                case STRING -> store.set(entry.key(), (String) entry.value());
                case LIST -> store.rPush(entry.key(), ((List<String>) entry.value()).toArray(new String[0]));
                case SET -> store.sAdd(entry.key(), ((Set<String>) entry.value()).toArray(new String[0]));
                case ZSET -> store.zAdd(entry.key(), (Map<String, Double>) entry.value());
                case HASH -> store.hMSet(entry.key(), (Map<String, String>) entry.value());
                default -> throw new IllegalStateException("Unsupported snapshot type: " + entry.type());
            }
            // 使用毫秒精度恢复 TTL，避免转换为秒时丢失不足一秒的有效期。
            if (remaining > 0) store.expireMs(entry.key(), remaining);
            count++;
        }
        return count;
    }

    private List<String> readList(ByteBuffer buffer) throws IOException {
        int count = readCount(buffer, "list");
        List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) values.add(requireValue(readString(buffer, MAX_VALUE_LENGTH, "list value")));
        return values;
    }

    private Set<String> readSet(ByteBuffer buffer) throws IOException {
        int count = readCount(buffer, "set");
        Set<String> values = new LinkedHashSet<>(capacity(count));
        for (int i = 0; i < count; i++) values.add(requireValue(readString(buffer, MAX_VALUE_LENGTH, "set member")));
        return values;
    }

    private Map<String, String> readHash(ByteBuffer buffer) throws IOException {
        int count = readCount(buffer, "hash");
        Map<String, String> values = new LinkedHashMap<>(capacity(count));
        for (int i = 0; i < count; i++) {
            String field = requireValue(readString(buffer, MAX_VALUE_LENGTH, "hash field"));
            String value = requireValue(readString(buffer, MAX_VALUE_LENGTH, "hash value"));
            values.put(field, value);
        }
        return values;
    }

    private Map<String, Double> readZSet(ByteBuffer buffer) throws IOException {
        int count = readCount(buffer, "sorted set");
        Map<String, Double> values = new LinkedHashMap<>(capacity(count));
        for (int i = 0; i < count; i++) {
            String member = requireValue(readString(buffer, MAX_VALUE_LENGTH, "sorted set member"));
            requireRemaining(buffer, Double.BYTES, "sorted set score");
            double score = buffer.getDouble();
            if (Double.isNaN(score)) throw new IOException("Invalid sorted set score");
            values.put(member, score);
        }
        return values;
    }

    private int readCount(ByteBuffer buffer, String kind) throws IOException {
        int count = readInt(buffer, kind + " element count");
        // 同时校验数量上限和最小所需字节数，阻止超大分配及明显截断的数据。
        if (count < 0 || count > MAX_COLLECTION_ENTRIES)
            throw new IOException("Invalid " + kind + " element count: " + count);
        if ((long) count * Integer.BYTES > buffer.remaining())
            throw new IOException("Truncated " + kind + " payload");
        return count;
    }

    private static String readString(ByteBuffer buffer, int maxLength, String label) throws IOException {
        int length = readInt(buffer, label + " length");
        if (length == -1) return null;
        // 分配 byte[] 之前先检查长度和剩余字节，避免 OOM 或 BufferUnderflow。
        if (length < 0 || length > maxLength) throw new IOException("Invalid " + label + " length: " + length);
        requireRemaining(buffer, length, label);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String requireValue(String value) throws IOException {
        if (value == null) throw new IOException("Collection values cannot be null");
        return value;
    }

    private static int readUnsignedByte(ByteBuffer buffer, String label) throws IOException {
        requireRemaining(buffer, 1, label);
        return buffer.get() & 0xFF;
    }

    private static int readInt(ByteBuffer buffer, String label) throws IOException {
        requireRemaining(buffer, Integer.BYTES, label);
        return buffer.getInt();
    }

    private static long readLong(ByteBuffer buffer, String label) throws IOException {
        requireRemaining(buffer, Long.BYTES, label);
        return buffer.getLong();
    }

    private static void requireRemaining(ByteBuffer buffer, int bytes, String label) throws IOException {
        // 所有基础读取最终都经过该方法，统一处理截断文件的边界检查。
        if (bytes < 0 || buffer.remaining() < bytes) throw new IOException("Truncated RDB " + label);
    }

    private static int capacity(int size) {
        return size < 3 ? size + 1 : (int) Math.min(Integer.MAX_VALUE, size / 0.75f + 1);
    }

    private static DataType type(int typeCode) throws IOException {
        return switch (typeCode) {
            case 0x00 -> DataType.STRING;
            case 0x01 -> DataType.LIST;
            case 0x02 -> DataType.SET;
            case 0x03 -> DataType.ZSET;
            case 0x04 -> DataType.HASH;
            default -> throw new IOException("Unknown RDB value type");
        };
    }

    public boolean exists(String filePath) { return new File(filePath).exists(); }
    public long getLastModified(String filePath) {
        File file = new File(filePath);
        return file.exists() ? file.lastModified() / 1000 : 0;
    }
    public long getLastSaveTimestamp() { return lastSaveTimestamp; }

    private record LoadedEntry(String key, DataType type, Object value, long expireAt) {}

    @FunctionalInterface
    private interface ValueReader {
        Object read(ByteBuffer buffer) throws IOException;
    }
}
