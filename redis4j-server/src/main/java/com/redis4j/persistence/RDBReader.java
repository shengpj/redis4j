package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * RDB 文件读取器 — NIO 版本
 *
 * 使用 FileChannel.map() 将文件映射到 MappedByteBuffer，
 * 直接在虚拟内存中解析，消除逐字节的 in.read() 系统调用。
 */
public class RDBReader {

    private static final Logger logger = LoggerFactory.getLogger(RDBReader.class);

    private static final int MAX_KEY_LENGTH = 10_000_000;

    private long lastSaveTimestamp = 0;
    private final Map<Integer, ValueReader> codecs = Map.of(
            0x00, this::restoreString,
            0x01, this::restoreList,
            0x02, this::restoreSet,
            0x04, this::restoreHash);

    public void load(DataStore dataStore, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.info("RDB file {} does not exist, skipping", filePath);
            return;
        }

        logger.info("Loading RDB file from {}", filePath);

        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());

            byte[] header = new byte[9];
            buffer.get(header);
            String headerStr = new String(header, 0, 9);
            if (!headerStr.startsWith("REDIS")) {
                logger.warn("Invalid RDB file header: {}", headerStr);
                return;
            }

            buffer.getInt(); // skip version
            int loaded;
            try {
                loaded = readDatabase(buffer, dataStore);
            } catch (BufferUnderflowException e) {
                logger.warn("RDB file is truncated or corrupted", e);
                loaded = -1;
            }
            lastSaveTimestamp = file.lastModified() / 1000;
            logger.info("RDB file loaded successfully: {} keys restored", loaded);
        }
    }

    /**
     * 从 MappedByteBuffer 解析数据库条目
     */
    private int readDatabase(MappedByteBuffer buffer, DataStore dataStore) {
        int count = 0;

        while (buffer.hasRemaining()) {
            int b = buffer.get() & 0xFF;

            if (b == 0xFF) {
                buffer.getInt(); // skip checksum
                break;
            }
            if (b == 0xFE) {
                buffer.getInt(); // skip DB number
                continue;
            }
            if (b != 0xC0) {
                continue; // skip unknown bytes
            }

            // === Read entry ===
            int keyLen = buffer.getInt();
            if (keyLen <= 0 || keyLen > MAX_KEY_LENGTH) continue;

            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // Read expire flag
            int hasExpire = buffer.get() & 0xFF;

            long expireAt = -1;
            if (hasExpire == 0x01) {
                expireAt = buffer.getLong();
            }

            // Read type AFTER optional expireAt (8 bytes consumed above)
            int type = buffer.get() & 0xFF;

            ValueReader codec = codecs.get(type);
            if (codec == null) {
                logger.warn("Unknown type 0x{} for key '{}'", Integer.toHexString(type), key);
            } else {
                count += codec.read(buffer, dataStore, key, expireAt);
            }
        }

        return count;
    }

    private int restoreString(MappedByteBuffer buffer, DataStore dataStore, String key, long expireAt) {
        String value = readString(buffer);
        if (value == null) return 0;

        if (expireAt > 0) {
            long seconds = (expireAt - System.currentTimeMillis()) / 1000;
            if (seconds > 0) {
                dataStore.setEx(key, value, seconds);
            } else {
                dataStore.set(key, value);
            }
        } else {
            dataStore.set(key, value);
        }
        return 1;
    }

    private int restoreList(MappedByteBuffer buffer, DataStore dataStore, String key, long expireAt) {
        int elemCount = buffer.getInt();
        for (int i = 0; i < elemCount; i++) {
            String v = readString(buffer);
            if (v != null) dataStore.rPush(key, v);
        }
        if (expireAt > 0) setExpire(dataStore, key, expireAt);
        return 1;
    }

    private int restoreSet(MappedByteBuffer buffer, DataStore dataStore, String key, long expireAt) {
        int memberCount = buffer.getInt();
        for (int i = 0; i < memberCount; i++) {
            String m = readString(buffer);
            if (m != null) dataStore.sAdd(key, m);
        }
        if (expireAt > 0) setExpire(dataStore, key, expireAt);
        return 1;
    }

    private int restoreHash(MappedByteBuffer buffer, DataStore dataStore, String key, long expireAt) {
        int fieldCount = buffer.getInt();
        for (int i = 0; i < fieldCount; i++) {
            String fld = readString(buffer);
            String fval = readString(buffer);
            if (fld != null && fval != null) dataStore.hSet(key, fld, fval);
        }
        if (expireAt > 0) setExpire(dataStore, key, expireAt);
        return 1;
    }

    private void setExpire(DataStore dataStore, String key, long expireAt) {
        long seconds = (expireAt - System.currentTimeMillis()) / 1000;
        if (seconds > 0) {
            dataStore.expire(key, seconds);
        }
    }

    private String readString(MappedByteBuffer buffer) {
        int len = buffer.getInt();
        if (len < 0) return null;
        // len == 0: 空字符串，new byte[0] 和 get 都是合法操作
        byte[] buf = new byte[len];
        buffer.get(buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    // ==================== Public query methods ====================

    public boolean exists(String filePath) {
        return new File(filePath).exists();
    }

    public long getLastModified(String filePath) {
        File file = new File(filePath);
        return file.exists() ? file.lastModified() / 1000 : 0;
    }

    public long getLastSaveTimestamp() {
        return lastSaveTimestamp;
    }

    @FunctionalInterface
    private interface ValueReader {
        int read(MappedByteBuffer buffer, DataStore dataStore, String key, long expireAt);
    }
}
