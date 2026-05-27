package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * RDB 文件读取器
 */
public class RDBReader {

    private static final Logger logger = LoggerFactory.getLogger(RDBReader.class);

    private long lastSaveTimestamp = 0;

    public void load(DataStore dataStore, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.info("RDB file {} does not exist, skipping", filePath);
            return;
        }

        logger.info("Loading RDB file from {}", filePath);

        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            byte[] header = new byte[9];
            if (bis.read(header) < 9) {
                logger.warn("Invalid RDB file: header too short");
                return;
            }

            String headerStr = new String(header, 0, 9);
            if (!headerStr.startsWith("REDIS")) {
                logger.warn("Invalid RDB file header: {}", headerStr);
                return;
            }

            bis.skip(4); // skip version
            int loaded = readDatabase(bis, dataStore);
            lastSaveTimestamp = file.lastModified() / 1000;
            logger.info("RDB file loaded successfully: {} keys restored", loaded);
        }
    }

    private int readDatabase(InputStream in, DataStore dataStore) throws IOException {
        int count = 0;

        while (true) {
            int b = in.read();
            if (b < 0) break;

            if (b == 0xFF) {
                in.skip(4); // checksum
                break;
            }

            if (b == 0xFE) {
                in.skip(4); // SELECTDB db_number
                continue;
            }

            if (b != 0xC0) {
                continue; // skip unknown bytes
            }

            // Read key: ENTRY_MARKER + KEY_LEN(4) + KEY_BYTES
            int keyLen = readInt32(in);
            if (keyLen < 0 || keyLen > 10_000_000) continue;
            byte[] keyBytes = new byte[keyLen];
            if (readFully(in, keyBytes) < keyLen) break;
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // Read has-expire flag
            int hasExpire = in.read();
            if (hasExpire < 0) break;

            long expireAt = -1;
            if (hasExpire == 0x01) {
                expireAt = readInt64(in);
            }

            // Read type AFTER optional expireAt (8 bytes consumed above)
            int type = in.read();
            if (type < 0) break;

            // Restore value
            switch (type) {
                case 0x00: {
                    String value = readString(in);
                    if (value == null) break;
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
                    count++;
                    break;
                }
                case 0x01: {
                    int elemCount = readInt32(in);
                    for (int i = 0; i < elemCount; i++) {
                        String v = readString(in);
                        if (v != null) dataStore.rPush(key, v);
                    }
                    if (expireAt > 0) setExpire(dataStore, key, expireAt);
                    count++;
                    break;
                }
                case 0x02: {
                    int memberCount = readInt32(in);
                    for (int i = 0; i < memberCount; i++) {
                        String m = readString(in);
                        if (m != null) dataStore.sAdd(key, m);
                    }
                    if (expireAt > 0) setExpire(dataStore, key, expireAt);
                    count++;
                    break;
                }
            case 0x04: {
                int fieldCount = readInt32(in);
                for (int i = 0; i < fieldCount; i++) {
                    String fld = readString(in);
                    String fval = readString(in);
                    if (fld != null && fval != null) dataStore.hSet(key, fld, fval);
                }
                if (expireAt > 0) setExpire(dataStore, key, expireAt);
                count++;
                break;
            }
                default:
                    logger.warn("Unknown type 0x{} for key '{}'", Integer.toHexString(type), key);
            }
        }

        return count;
    }

    private void setExpire(DataStore dataStore, String key, long expireAt) {
        long seconds = (expireAt - System.currentTimeMillis()) / 1000;
        if (seconds > 0) {
            dataStore.expire(key, seconds);
        }
    }

    private String readString(InputStream in) throws IOException {
        int len = readInt32(in);
        if (len < 0) return null;
        byte[] buf = new byte[len];
        if (readFully(in, buf) < len) return null;
        return new String(buf, StandardCharsets.UTF_8);
    }

    private int readInt32(InputStream in) throws IOException {
        byte[] buf = new byte[4];
        if (readFully(in, buf) < 4) return -1;
        return ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16)
                | ((buf[2] & 0xFF) << 8) | (buf[3] & 0xFF);
    }

    private long readInt64(InputStream in) throws IOException {
        byte[] buf = new byte[8];
        if (readFully(in, buf) < 8) return 0;
        return ((buf[0] & 0xFFL) << 56) | ((buf[1] & 0xFFL) << 48)
                | ((buf[2] & 0xFFL) << 40) | ((buf[3] & 0xFFL) << 32)
                | ((buf[4] & 0xFFL) << 24) | ((buf[5] & 0xFFL) << 16)
                | ((buf[6] & 0xFFL) << 8) | (buf[7] & 0xFFL);
    }

    private int readFully(InputStream in, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int n = in.read(buf, total, buf.length - total);
            if (n < 0) return total;
            total += n;
        }
        return total;
    }

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
}
