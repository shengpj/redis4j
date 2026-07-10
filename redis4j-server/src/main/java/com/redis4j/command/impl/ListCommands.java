package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.ListStore;
import com.redis4j.protocol.response.CommandResponse;

import java.util.Arrays;

/**
 * List 鍛戒护瀹炵幇
 */
public class ListCommands {

    // ==================== LPUSH ====================

    @RedisCommand
    public static class ListLPushCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLPushCommand(ListStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LPUSH";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 2 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.lPush(key, values);
            return CommandResponses.integer(length);
        }
    }

    // ==================== RPUSH ====================

    @RedisCommand
    public static class ListRPushCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListRPushCommand(ListStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RPUSH";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 2 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.rPush(key, values);
            return CommandResponses.integer(length);
        }
    }

    // ==================== LPOP ====================

    @RedisCommand
    public static class ListLPopCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLPopCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String value = dataStore.lPop(args[0]);
            return CommandResponses.bulkString(value);
        }
    }

    // ==================== RPOP ====================

    @RedisCommand
    public static class ListRPopCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListRPopCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String value = dataStore.rPop(args[0]);
            return CommandResponses.bulkString(value);
        }
    }

    // ==================== LLEN ====================

    @RedisCommand
    public static class ListLLenCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLLenCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long length = dataStore.lLen(args[0]);
            return CommandResponses.integer(length);
        }
    }

    // ==================== LRANGE ====================

    @RedisCommand
    public static class ListLRangeCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLRangeCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            long start = parseLong(args[1]);
            long stop = parseLong(args[2]);
            String[] values = dataStore.lRange(key, start, stop);
            return CommandResponses.array(Arrays.stream(values)
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== LINDEX ====================

    @RedisCommand
    public static class ListLIndexCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLIndexCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long index = parseLong(args[1]);
            String value = dataStore.lIndex(args[0], index);
            return CommandResponses.bulkString(value);
        }
    }

    // ==================== LSET ====================

    @RedisCommand
    public static class ListLSetCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLSetCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long index = parseLong(args[1]);
            String value = args[2];
            dataStore.lSet(args[0], index, value);
            return CommandResponses.ok();
        }
    }

    // ==================== LTRIM ====================

    @RedisCommand
    public static class ListLTrimCommand extends AbstractCommand {
        private final ListStore dataStore;

        public ListLTrimCommand(ListStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long start = parseLong(args[1]);
            long stop = parseLong(args[2]);
            dataStore.lTrim(args[0], start, stop);
            return CommandResponses.ok();
        }
    }
}
