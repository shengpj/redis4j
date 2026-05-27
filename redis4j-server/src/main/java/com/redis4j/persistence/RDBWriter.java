package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

/**
 * RDB 文件写入器 — NIO 版本
 *
 * 使用 FileChannel + ByteBuffer 替代 BIO 的 FileOutputStream/BufferedOutputStream，
 * 消除每次写操作的系统调用。写入过程不创建中间 Entry/RedisValue 对象，
 * 直接从 DataStore 流式序列化到 ByteBuffer。
 *
 * 格式（大端字节序）：
 *   HEADER(9) VERSION(4)
 *   SELECTDB(1) DB_NUM(4)
 *   [ENTRY]...
 *   EOF(1) CHECKSUM(4)
 *
 * ENTRY 格式：
 *   MARKER(1=0xC0) KEY_LEN(4) KEY_BYTES EXPIRE_FLAG(1) [EXPIRE_MS(8)] TYPE(1) VALUE...
 *
 * TYPE 格式：
 *   0x00 = String:  VAL_LEN(4) VAL_BYTES
 *   0x01 = List:     COUNT(4) [VAL_LEN(4) VAL_BYTES]...
 *   0x02 = Set:      COUNT(4) [VAL_LEN(4) VAL_BYTES]...
 *   0x04 = Hash:     COUNT(4) [FLD_LEN(4) FLD_BYTES VAL_LEN(4) VAL_BYTES]...
 */
public class RDBWriter {

    private static final Logger logger = LoggerFactory.getLogger(RDBWriter.class);

    private static final byte[] RDB_HEADER = "REDIS0011".getBytes();
    private static final byte ENTRY_MARKER = (byte) 0xC0;
    private static final byte TYPE_STRING = 0x00;
    private static final byte TYPE_LIST = 0x01;
    private static final byte TYPE_SET = 0x02;
    private static final byte TYPE_HASH = 0x04;

    private static final int BUFFER_SIZE = 64 * 1024;

    private FileChannel channel;
    private ByteBuffer buffer;

    public void save(DataStore dataStore, String filePath) throws IOException {
        logger.info("Saving RDB file to {}", filePath);

        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        long start = System.currentTimeMillis();

        try (FileChannel ch = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            this.channel = ch;
            this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

            writeHeader();
            writeKeyValues(dataStore);
            writeFooter();
            flushBuffer();
            channel.force(true);
        }

        long elapsed = System.currentTimeMillis() - start;
        logger.info("RDB file saved successfully in {} ms: {}", elapsed, filePath);
    }

    // ==================== Buffer management ====================

    private void flushBuffer() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        buffer.clear();
    }

    private void ensure(int bytes) throws IOException {
        if (buffer.remaining() < bytes) {
            flushBuffer();
        }
    }

    // ==================== Write primitives ====================

    private void writeInt32(int val) throws IOException {
        ensure(4);
        buffer.putInt(val);
    }

    private void writeInt64(long val) throws IOException {
        ensure(8);
        buffer.putLong(val);
    }

    private void writeString(String value) throws IOException {
        if (value == null) {
            ensure(4);
            buffer.putInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ensure(4 + bytes.length);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    // ==================== File structure ====================

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

    // ==================== Key-value serialization ====================

    /**
     * 遍历所有 key 并直接序列化到 ByteBuffer。
     * 不创建任何中间 Entry / RedisList / RedisSet / RedisHash 对象。
     */
    private void writeKeyValues(DataStore dataStore) throws IOException {
        Set<String> keys = dataStore.getAllKeys();
        int count = 0;
        for (String key : keys) {
            DataType type = dataStore.type(key);
            if (type == DataType.NONE || type == DataType.ZSET) continue;

            long ttl = dataStore.ttl(key);
            long expireAt = ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1;

            // 跳过已过期的 key
            if (expireAt > 0 && expireAt <= System.currentTimeMillis()) continue;

            // 预分配常见条目的估算空间：标记 + key + expire + 类型头
            ensure(512);

            buffer.put(ENTRY_MARKER);
            writeString(key);
            boolean hasExpire = expireAt > 0;
            buffer.put(hasExpire ? (byte) 0x01 : (byte) 0x00);
            if (hasExpire) {
                buffer.putLong(expireAt);
            }

            switch (type) {
                case STRING -> writeStringValue(dataStore, key);
                case LIST -> writeListValue(dataStore, key);
                case SET -> writeSetValue(dataStore, key);
                case HASH -> writeHashValue(dataStore, key);
            }
            count++;
        }
        logger.debug("Wrote {} key-value pairs to RDB", count);
    }

    private void writeStringValue(DataStore dataStore, String key) throws IOException {
        buffer.put(TYPE_STRING);
        String value = dataStore.get(key);
        writeString(value);
    }

    private void writeListValue(DataStore dataStore, String key) throws IOException {
        buffer.put(TYPE_LIST);
        String[] elems = dataStore.lRange(key, 0, -1);
        writeInt32(elems.length);
        for (String elem : elems) {
            writeString(elem);
        }
    }

    private void writeSetValue(DataStore dataStore, String key) throws IOException {
        buffer.put(TYPE_SET);
        Set<String> members = dataStore.sMembers(key);
        writeInt32(members.size());
        for (String member : members) {
            writeString(member);
        }
    }

    private void writeHashValue(DataStore dataStore, String key) throws IOException {
        buffer.put(TYPE_HASH);
        Map<String, String> fields = dataStore.hGetAll(key);
        writeInt32(fields.size());
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }
    }
}
