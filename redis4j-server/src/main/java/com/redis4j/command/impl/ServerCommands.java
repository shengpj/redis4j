package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.persistence.PersistenceManager;
import com.redis4j.persistence.aof.AofManager;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.command.CommandRegistry;
import com.redis4j.server.ServerConfig;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.memory.MemoryManagedStore;
import com.redis4j.storage.snapshot.SnapshotProvider;

import java.util.Locale;

/**
 * 服务器控制命令实现
 */
public class ServerCommands {

    /** INFO MEMORY - 返回内存限制模块使用的估算内存指标。 */
    public static class InfoCommand extends AbstractCommand {
        private final DataStore dataStore;
        private final ServerConfig config;

        public InfoCommand(DataStore dataStore, ServerConfig config) {
            this.dataStore = dataStore;
            this.config = config;
        }

        @Override
        public String getName() {
            return "INFO";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            if (!"MEMORY".equalsIgnoreCase(args[0])) {
                return CommandResponses.error("ERR unsupported INFO section '" + args[0] + "'");
            }
            long usedMemory = dataStore instanceof MemoryManagedStore store
                    ? store.estimatedMemoryUsage() : 0;
            long maximumMemory = config.getMaxMemoryBytes();
            String info = "# Memory\r\n"
                    + "used_memory:" + usedMemory + "\r\n"
                    + "used_memory_human:" + humanBytes(usedMemory) + "\r\n"
                    + "used_memory_estimated:1\r\n"
                    + "maxmemory:" + maximumMemory + "\r\n"
                    + "maxmemory_human:" + humanBytes(maximumMemory) + "\r\n"
                    + "maxmemory_policy:" + config.getMaxMemoryPolicy().name().toLowerCase(Locale.ROOT)
                            .replace('_', '-') + "\r\n"
                    + "db0_keys:" + dataStore.dbSize() + "\r\n";
            return CommandResponses.bulkString(info);
        }

        private static String humanBytes(long bytes) {
            if (bytes < 1024) return bytes + "B";
            if (bytes < 1024L * 1024) return String.format(Locale.ROOT, "%.2fK", bytes / 1024.0);
            if (bytes < 1024L * 1024 * 1024) return String.format(Locale.ROOT, "%.2fM", bytes / (1024.0 * 1024));
            return String.format(Locale.ROOT, "%.2fG", bytes / (1024.0 * 1024 * 1024));
        }
    }

    /** BGREWRITEAOF - 后台生成紧凑 AOF 文件。 */
    public static class BgRewriteAofCommand extends AbstractCommand {
        private final AofManager aofManager;
        private final CommandRegistry commandRegistry;
        private final SnapshotProvider snapshotProvider;

        public BgRewriteAofCommand(AofManager aofManager, CommandRegistry commandRegistry,
                                   SnapshotProvider snapshotProvider) {
            this.aofManager = aofManager;
            this.commandRegistry = commandRegistry;
            this.snapshotProvider = snapshotProvider;
        }

        @Override
        public String getName() {
            return "BGREWRITEAOF";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            if (!aofManager.bgRewrite(commandRegistry, snapshotProvider)) {
                return CommandResponses.error("ERR AOF rewrite already in progress or AOF is unavailable");
            }
            return CommandResponses.simpleString("Background append only file rewriting started");
        }
    }

    /**
     * SAVE - 同步阻塞保存 RDB 快照
     */
    @RedisCommand
    public static class SaveCommand extends AbstractCommand {
        private final PersistenceManager pm;

        public SaveCommand(PersistenceManager pm) {
            this.pm = pm;
        }

        @Override
        public String getName() {
            return "SAVE";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            if (pm.isSaving()) {
                return CommandResponses.error("ERR BGSAVE already in progress");
            }
            pm.save();
            return CommandResponses.ok();
        }
    }

    /**
     * BGSAVE - 异步后台保存 RDB 快照
     */
    @RedisCommand
    public static class BgSaveCommand extends AbstractCommand {
        private final PersistenceManager pm;

        public BgSaveCommand(PersistenceManager pm) {
            this.pm = pm;
        }

        @Override
        public String getName() {
            return "BGSAVE";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            if (pm.isSaving()) {
                return CommandResponses.error("ERR BGSAVE already in progress");
            }
            pm.bgSaveManual();
            return CommandResponses.simpleString("Background saving started");
        }
    }

    /**
     * LASTSAVE - 返回最后一次成功 SAVE 的 Unix 时间戳（秒）
     */
    @RedisCommand
    public static class LastSaveCommand extends AbstractCommand {
        private final PersistenceManager pm;

        public LastSaveCommand(PersistenceManager pm) {
            this.pm = pm;
        }

        @Override
        public String getName() {
            return "LASTSAVE";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            return CommandResponses.integer(pm.getLastSaveTimestamp());
        }
    }
}
