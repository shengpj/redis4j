package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * RDB 文件写入器
 */
public class RDBWriter {

    private static final Logger logger = LoggerFactory.getLogger(RDBWriter.class);

    private static final byte[] RDB_HEADER = "REDIS0011".getBytes();
    private static final byte[] EOF_MARK = new byte[40];

    /**
     * 将数据存储写入 RDB 文件
     */
    public void save(DataStore dataStore, String filePath) throws IOException {
        logger.info("Saving RDB file to {}", filePath);

        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (FileOutputStream fos = new FileOutputStream(file);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            // 写入 RDB 头
            bos.write(RDB_HEADER);

            // 写入数据库选择器（数据库 0）
            bos.write(0xFF); // SELECTDB
            bos.write(("0".length()));
            bos.write(String.valueOf(0).getBytes());
            bos.write(0xFE); // EOF SELECTDB

            // 写入 key-value 数据
            writeKeyValues(dataStore, bos);

            // 写入 EOF 标记
            bos.write(EOF_MARK);

            bos.flush();
        }

        logger.info("RDB file saved successfully");
    }

    /**
     * 写入 key-value 数据
     */
    private void writeKeyValues(DataStore dataStore, OutputStream out) throws IOException {
        // 注意：这里需要 DataStore 提供遍历所有 key 的方法
        // 由于当前 DataStore 接口没有提供完整遍历，我们简化处理
        // 实际实现中应该遍历所有 key 并根据类型写入

        // 写入数据库大小信息（用于快速恢复）
        // 格式: DBSIZE <db_size>
        long dbSize = dataStore.dbSize();
        writeString(out, "dbsize:" + dbSize);
    }

    /**
     * 写入字符串
     */
    private void writeString(OutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes();
        out.write(bytes.length);
        out.write(bytes);
    }

    /**
     * 写入字节数组
     */
    private void writeBytes(OutputStream out, byte[] value) throws IOException {
        out.write(value.length >>> 24);
        out.write(value.length >>> 16);
        out.write(value.length >>> 8);
        out.write(value.length);
        out.write(value);
    }
}
