package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataType;
import com.redis4j.storage.Entry;
import com.redis4j.storage.type.RedisHash;
import com.redis4j.storage.type.RedisList;
import com.redis4j.storage.type.RedisSet;
import com.redis4j.storage.type.RedisString;
import com.redis4j.storage.type.RedisValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/**
 * RDB 文件写入器
 * 格式（无歧义）：
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

    public void save(DataStore dataStore, String filePath) throws IOException {
        logger.info("Saving RDB file to {}", filePath);

        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        long start = System.currentTimeMillis();

        try (FileOutputStream fos = new FileOutputStream(file);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            bos.write(RDB_HEADER);
            writeInt32(bos, 9);

            bos.write(0xFE);
            writeInt32(bos, 0);

            writeKeyValues(dataStore, bos);

            bos.write(0xFF);
            writeInt32(bos, 0);
            bos.flush();
        }

        long elapsed = System.currentTimeMillis() - start;
        logger.info("RDB file saved successfully in {} ms: {}", elapsed, filePath);
    }

    private void writeKeyValues(DataStore dataStore, OutputStream out) throws IOException {
        Set<String> keys = dataStore.getAllKeys();
        int count = 0;
        for (String key : keys) {
            Entry entry = createLiveEntry(dataStore, key);
            if (entry == null || entry.isExpired()) continue;

            out.write(ENTRY_MARKER);
            writeString(out, key);
            boolean hasExpire = entry.getExpireAt() > 0;
            out.write(hasExpire ? (byte) 0x01 : (byte) 0x00);
            if (hasExpire) {
                writeInt64(out, entry.getExpireAt());
            }
            writeValue(out, entry.getValue());
            count++;
        }
        logger.debug("Wrote {} key-value pairs to RDB", count);
    }

    private Entry createLiveEntry(DataStore dataStore, String key) {
        DataType type = dataStore.type(key);
        switch (type) {
            case STRING: {
                String v = dataStore.get(key);
                if (v == null) return null;
                long ttl = dataStore.ttl(key);
                long expireAt = ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1;
                return new Entry(new RedisString(v), expireAt);
            }
            case LIST: {
                String[] elems = dataStore.lRange(key, 0, -1);
                if (elems == null || elems.length == 0) return null;
                RedisList list = new RedisList();
                for (String e : elems) list.add(e);
                long ttl = dataStore.ttl(key);
                long expireAt = ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1;
                return new Entry(list, expireAt);
            }
            case SET: {
                Set<String> members = dataStore.sMembers(key);
                if (members == null || members.isEmpty()) return null;
                RedisSet set = new RedisSet(members);
                long ttl = dataStore.ttl(key);
                long expireAt = ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1;
                return new Entry(set, expireAt);
            }
            case HASH: {
                Map<String, String> fields = dataStore.hGetAll(key);
                if (fields == null || fields.isEmpty()) return null;
                RedisHash hash = new RedisHash(fields);
                long ttl = dataStore.ttl(key);
                long expireAt = ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1;
                return new Entry(hash, expireAt);
            }
            default:
                return null;
        }
    }

    private void writeValue(OutputStream out, RedisValue value) throws IOException {
        if (value instanceof RedisString) {
            out.write(TYPE_STRING);
            writeString(out, ((RedisString) value).getStringValue());
        } else if (value instanceof RedisList) {
            RedisList list = (RedisList) value;
            out.write(TYPE_LIST);
            writeInt32(out, (int) list.size());
            for (String elem : list) {
                writeString(out, elem);
            }
        } else if (value instanceof RedisSet) {
            RedisSet set = (RedisSet) value;
            out.write(TYPE_SET);
            writeInt32(out, (int) set.size());
            for (String member : set) {
                writeString(out, member);
            }
        } else if (value instanceof RedisHash) {
            RedisHash hash = (RedisHash) value;
            out.write(TYPE_HASH);
            writeInt32(out, (int) hash.size());
            for (Map.Entry<String, String> e : hash.entries()) {
                writeString(out, e.getKey());
                writeString(out, e.getValue());
            }
        }
    }

    private void writeString(OutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeInt32(out, bytes.length);
        out.write(bytes);
    }

    private void writeInt32(OutputStream out, int val) throws IOException {
        out.write((val >>> 24) & 0xFF);
        out.write((val >>> 16) & 0xFF);
        out.write((val >>> 8) & 0xFF);
        out.write(val & 0xFF);
    }

    private void writeInt64(OutputStream out, long val) throws IOException {
        out.write((int) ((val >>> 56) & 0xFF));
        out.write((int) ((val >>> 48) & 0xFF));
        out.write((int) ((val >>> 40) & 0xFF));
        out.write((int) ((val >>> 32) & 0xFF));
        out.write((int) ((val >>> 24) & 0xFF));
        out.write((int) ((val >>> 16) & 0xFF));
        out.write((int) ((val >>> 8) & 0xFF));
        out.write((int) (val & 0xFF));
    }
}
