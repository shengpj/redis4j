package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 持久化管理器
 */
public class PersistenceManager {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);

    private final DataStore dataStore;
    private final String dataDir;
    private final RDBWriter rdbWriter;
    private final RDBReader rdbReader;

    private ScheduledExecutorService scheduler;
    private long saveIntervalSeconds = 900; // 默认 15 分钟
    private boolean enabled = true;

    public PersistenceManager(DataStore dataStore, String dataDir) {
        this.dataStore = dataStore;
        this.dataDir = dataDir;
        this.rdbWriter = new RDBWriter();
        this.rdbReader = new RDBReader();
    }

    /**
     * 启动定时持久化任务
     */
    public void start() {
        if (!enabled) {
            return;
        }

        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "rdb-save");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                save();
            } catch (Exception e) {
                logger.error("Error during RDB save", e);
            }
        }, saveIntervalSeconds, saveIntervalSeconds, TimeUnit.SECONDS);

        logger.info("Persistence manager started with interval {} seconds", saveIntervalSeconds);
    }

    /**
     * 停止持久化管理器
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }
    }

    /**
     * 执行 RDB 保存
     */
    public void save() {
        String filePath = dataDir + "/dump.rdb";
        try {
            rdbWriter.save(dataStore, filePath);
        } catch (IOException e) {
            logger.error("Failed to save RDB file", e);
        }
    }

    /**
     * 加载 RDB 文件
     */
    public void load() {
        String filePath = dataDir + "/dump.rdb";
        try {
            rdbReader.load(dataStore, filePath);
        } catch (IOException e) {
            logger.error("Failed to load RDB file", e);
        }
    }

    /**
     * 设置保存间隔（秒）
     */
    public void setSaveInterval(long seconds) {
        this.saveIntervalSeconds = seconds;
    }

    /**
     * 设置是否启用持久化
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
