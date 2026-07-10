package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.persistence.PersistenceManager;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.protocol.response.CommandResponse;

/**
 * 服务器控制命令实现
 */
public class ServerCommands {

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
