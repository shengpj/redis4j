package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 持久化管理器
 * 负责 RDB 定时快照和启动时数据恢复
 */
public class PersistenceManager {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);

    private final DataStore dataStore;
    private final String dataDir;
    private final RDBWriter rdbWriter;
    private final RDBReader rdbReader;

    private ScheduledExecutorService scheduler;
    private volatile long saveIntervalSeconds = 900;
    private volatile boolean enabled = true;
    private final AtomicBoolean isSaving = new AtomicBoolean(false);
    private final AtomicLong lastSaveTimestamp = new AtomicLong(0);
    private volatile long lastBgSaveStart = 0;

    public PersistenceManager(DataStore dataStore, String dataDir) {
        this.dataStore = dataStore;
        this.dataDir = dataDir;
        this.rdbWriter = new RDBWriter();
        this.rdbReader = new RDBReader();
    }

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
                bgSave();
            } catch (Exception e) {
                logger.error("Error during scheduled RDB save", e);
            }
        }, saveIntervalSeconds, saveIntervalSeconds, TimeUnit.SECONDS);

        logger.info("Persistence manager started with interval {} seconds", saveIntervalSeconds);
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 同步保存（RDB 文件路径由 saveInterval 配置决定）
     */
    public void save() {
        save(dataDir + "/dump.rdb");
    }

    /**
     * 同步保存到指定路径
     */
    public void save(String filePath) {
        if (!isSaving.compareAndSet(false, true)) {
            logger.warn("RDB save already in progress, skipping");
            return;
        }
        try {
            rdbWriter.save(dataStore, filePath);
            lastSaveTimestamp.set(System.currentTimeMillis() / 1000);
        } catch (IOException e) {
            logger.error("Failed to save RDB file", e);
        } finally {
            isSaving.set(false);
        }
    }

    /**
     * 后台异步保存（由定时器触发）
     */
    private void bgSave() {
        if (!isSaving.compareAndSet(false, true)) {
            logger.debug("RDB save already in progress, skipping scheduled save");
            return;
        }
        long start = System.currentTimeMillis();
        try {
            String filePath = dataDir + "/dump.rdb";
            rdbWriter.save(dataStore, filePath);
            lastSaveTimestamp.set(System.currentTimeMillis() / 1000);
            long elapsed = System.currentTimeMillis() - start;
            logger.info("Background RDB save completed in {} ms", elapsed);
        } catch (IOException e) {
            logger.error("Background RDB save failed", e);
        } finally {
            isSaving.set(false);
        }
    }

    /**
     * 手动后台保存（由 BGSAVE 命令触发）
     */
    public void bgSaveManual() {
        lastBgSaveStart = System.currentTimeMillis() / 1000;
        Thread t = new Thread(() -> {
            if (!isSaving.compareAndSet(false, true)) {
                logger.warn("BGSAVE: save already in progress");
                return;
            }
            try {
                String filePath = dataDir + "/dump.rdb";
                rdbWriter.save(dataStore, filePath);
                lastSaveTimestamp.set(System.currentTimeMillis() / 1000);
                logger.info("BGSAVE completed");
            } catch (IOException e) {
                logger.error("BGSAVE failed", e);
            } finally {
                isSaving.set(false);
            }
        }, "rdb-bgsave");
        t.setDaemon(true);
        t.start();
    }

    public void load() {
        String filePath = dataDir + "/dump.rdb";
        try {
            rdbReader.load(dataStore, filePath);
            lastSaveTimestamp.set(rdbReader.getLastSaveTimestamp());
        } catch (IOException e) {
            logger.error("Failed to load RDB file", e);
        }
    }

    public void setSaveInterval(long seconds) {
        this.saveIntervalSeconds = seconds;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getLastSaveTimestamp() {
        return lastSaveTimestamp.get();
    }

    public long getLastBgSaveStart() {
        return lastBgSaveStart;
    }

    public boolean isSaving() {
        return isSaving.get();
    }
}
