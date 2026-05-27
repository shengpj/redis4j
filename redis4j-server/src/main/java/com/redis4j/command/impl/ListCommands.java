package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;

/**
 * List 命令实现
 */
public class ListCommands {

    // ==================== LPUSH ====================

    @RedisCommand(name = "LPUSH", arity = -2)
    public static class ListLPushCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLPushCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LPUSH";
        }

        @Override
        public int getArity() {
            return -2; // 至少 2 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.lPush(key, values);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== RPUSH ====================

    @RedisCommand(name = "RPUSH", arity = -2)
    public static class ListRPushCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListRPushCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RPUSH";
        }

        @Override
        public int getArity() {
            return -2; // 至少 2 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.rPush(key, values);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== LPOP ====================

    @RedisCommand(name = "LPOP", arity = 2)
    public static class ListLPopCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLPopCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LPOP";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String value = dataStore.lPop(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== RPOP ====================

    @RedisCommand(name = "RPOP", arity = 2)
    public static class ListRPopCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListRPopCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RPOP";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String value = dataStore.rPop(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== LLEN ====================

    @RedisCommand(name = "LLEN", arity = 2)
    public static class ListLLenCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLLenCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LLEN";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long length = dataStore.lLen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== LRANGE ====================

    @RedisCommand(name = "LRANGE", arity = 4)
    public static class ListLRangeCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLRangeCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LRANGE";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            long start = Long.parseLong(args[1]);
            long stop = Long.parseLong(args[2]);
            String[] values = dataStore.lRange(key, start, stop);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== LINDEX ====================

    @RedisCommand(name = "LINDEX", arity = 3)
    public static class ListLIndexCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLIndexCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LINDEX";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long index = Long.parseLong(args[1]);
            String value = dataStore.lIndex(args[0], index);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== LSET ====================

    @RedisCommand(name = "LSET", arity = 4)
    public static class ListLSetCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LSET";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long index = Long.parseLong(args[1]);
            String value = args[2];
            dataStore.lSet(args[0], index, value);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== LTRIM ====================

    @RedisCommand(name = "LTRIM", arity = 4)
    public static class ListLTrimCommand extends AbstractCommand {
        private final DataStore dataStore;

        public ListLTrimCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LTRIM";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long start = Long.parseLong(args[1]);
            long stop = Long.parseLong(args[2]);
            dataStore.lTrim(args[0], start, stop);
            return RedisMessageHelper.ok();
        }
    }
}
