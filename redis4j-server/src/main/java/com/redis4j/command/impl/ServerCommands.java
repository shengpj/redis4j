package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.persistence.PersistenceManager;
import com.redis4j.persistence.aof.AofManager;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.command.CommandRegistry;
import com.redis4j.storage.snapshot.SnapshotProvider;

/**
 * 服务器控制命令实现
 */
public class ServerCommands {

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
