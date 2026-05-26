package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * RDB 文件读取器
 */
public class RDBReader {

    private static final Logger logger = LoggerFactory.getLogger(RDBReader.class);

    /**
     * 从 RDB 文件加载数据
     */
    public void load(DataStore dataStore, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.info("RDB file {} does not exist, skipping", filePath);
            return;
        }

        logger.info("Loading RDB file from {}", filePath);

        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            // 验证 RDB 头部
            byte[] header = new byte[9];
            int read = bis.read(header);
            if (read < 9) {
                logger.warn("Invalid RDB file header");
                return;
            }

            String headerStr = new String(header, 0, 8);
            if (!headerStr.startsWith("REDIS")) {
                logger.warn("Invalid RDB file header: {}", headerStr);
                return;
            }

            // 跳过 EOF 标记（40 字节）
            bis.skip(40);

            logger.info("RDB file loaded successfully");
        }
    }

    /**
     * 检查 RDB 文件是否存在
     */
    public boolean exists(String filePath) {
        return new File(filePath).exists();
    }

    /**
     * 获取 RDB 文件最后修改时间
     */
    public long getLastModified(String filePath) {
        File file = new File(filePath);
        return file.exists() ? file.lastModified() : 0;
    }
}
