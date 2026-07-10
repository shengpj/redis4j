package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.StringStore;
import com.redis4j.protocol.response.CommandResponse;

import java.util.Arrays;
import java.util.List;

/**
 * String 鍛戒护瀹炵幇
 */
public class StringCommands {

    // ==================== GET ====================

    @RedisCommand
    public static class StringGetCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringGetCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "GET";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String value = dataStore.get(args[0]);
            return CommandResponses.bulkString(value);
        }
    }

    // ==================== SET ====================

    @RedisCommand
    public static class StringSetCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringSetCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SET";
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
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'set' command (at least 2 required)";
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            String value = args[1];
            dataStore.set(key, value);
            return CommandResponses.ok();
        }
    }

    // ==================== SETNX ====================

    @RedisCommand
    public static class StringSetNxCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringSetNxCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SETNX";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            boolean result = dataStore.setNx(args[0], args[1]);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== SETEX ====================

    @RedisCommand
    public static class StringSetExCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringSetExCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SETEX";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            long seconds = parseLong(args[1]);
            String value = args[2];
            dataStore.setEx(key, value, seconds);
            return CommandResponses.ok();
        }

        @Override
        protected void logExecution(String[] args, CommandResponse result) {
            // SETEX 涓嶉渶瑕佽缁嗘棩蹇?
        }
    }

    // ==================== MGET ====================

    @RedisCommand
    public static class StringMGetCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringMGetCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "MGET";
        }

        @Override
        public int getArity() {
            return -2;
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'mget' command (at least 1 required)";
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String[] values = dataStore.mGet(args);
            return CommandResponses.array(Arrays.stream(values)
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== MSET ====================

    @RedisCommand
    public static class StringMSetCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringMSetCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "MSET";
        }

        @Override
        public int getArity() {
            return -2;
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2 && args.length % 2 == 0;
        }

        @Override
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'mset' command (even number of arguments required)";
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            java.util.Map<String, String> map = new java.util.HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                map.put(args[i], args[i + 1]);
            }
            dataStore.mSet(map);
            return CommandResponses.ok();
        }
    }

    // ==================== INCR ====================

    @RedisCommand
    public static class StringIncrCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringIncrCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "INCR";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long result = dataStore.incr(args[0]);
            return CommandResponses.integer(result);
        }
    }

    // ==================== INCRBY ====================

    @RedisCommand
    public static class StringIncrByCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringIncrByCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "INCRBY";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long delta = parseLong(args[1]);
            long result = dataStore.incrBy(args[0], delta);
            return CommandResponses.integer(result);
        }
    }

    // ==================== DECR ====================

    @RedisCommand
    public static class StringDecrCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringDecrCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DECR";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long result = dataStore.decr(args[0]);
            return CommandResponses.integer(result);
        }
    }

    // ==================== DECRBY ====================

    @RedisCommand
    public static class StringDecrByCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringDecrByCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DECRBY";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long delta = parseLong(args[1]);
            long result = dataStore.decrBy(args[0], delta);
            return CommandResponses.integer(result);
        }
    }

    // ==================== STRLEN ====================

    @RedisCommand
    public static class StringStrlenCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringStrlenCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "STRLEN";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long length = dataStore.strlen(args[0]);
            return CommandResponses.integer(length);
        }
    }

    // ==================== APPEND ====================

    @RedisCommand
    public static class StringAppendCommand extends AbstractCommand {
        private final StringStore dataStore;

        public StringAppendCommand(StringStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "APPEND";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long length = dataStore.append(args[0], args[1]);
            return CommandResponses.integer(length);
        }
    }
}
