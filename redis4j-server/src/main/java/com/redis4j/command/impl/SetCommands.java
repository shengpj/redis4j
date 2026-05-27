package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;
import java.util.Set;

/**
 * Set 命令实现
 */
public class SetCommands {

    // ==================== SADD ====================

    @RedisCommand(name = "SADD", arity = -2)
    public static class SetSAddCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSAddCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SADD";
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
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sAdd(key, members);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SREM ====================

    @RedisCommand(name = "SREM", arity = -2)
    public static class SetSRemCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSRemCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SREM";
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
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sRem(key, members);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SMEMBERS ====================

    @RedisCommand(name = "SMEMBERS", arity = 2)
    public static class SetSMembersCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSMembersCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SMEMBERS";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> members = dataStore.sMembers(args[0]);
            return RedisMessageHelper.array(members.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SISMEMBER ====================

    @RedisCommand(name = "SISMEMBER", arity = 3)
    public static class SetSIsMemberCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSIsMemberCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SISMEMBER";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            boolean isMember = dataStore.sIsMember(args[0], args[1]);
            return RedisMessageHelper.integer(isMember ? 1 : 0);
        }
    }

    // ==================== SCARD ====================

    @RedisCommand(name = "SCARD", arity = 2)
    public static class SetSCardCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSCardCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SCARD";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long count = dataStore.sCard(args[0]);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SINTER ====================

    @RedisCommand(name = "SINTER", arity = -2)
    public static class SetSInterCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSInterCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SINTER";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> result = dataStore.sInter(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SUNION ====================

    @RedisCommand(name = "SUNION", arity = -2)
    public static class SetSUnionCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSUnionCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SUNION";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> result = dataStore.sUnion(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SDIFF ====================

    @RedisCommand(name = "SDIFF", arity = -2)
    public static class SetSDiffCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSDiffCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SDIFF";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> result = dataStore.sDiff(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SMOVE ====================

    @RedisCommand(name = "SMOVE", arity = 4)
    public static class SetSMoveCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSMoveCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SMOVE";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            boolean result = dataStore.sMove(args[0], args[1], args[2]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== SPOP ====================

    @RedisCommand(name = "SPOP", arity = 2)
    public static class SetSPopCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSPopCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SPOP";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String member = dataStore.sPop(args[0]);
            return RedisMessageHelper.bulkString(member);
        }
    }

    // ==================== SRANDMEMBER ====================

    @RedisCommand(name = "SRANDMEMBER", arity = -2)
    public static class SetSRandMemberCommand extends AbstractCommand {
        private final DataStore dataStore;

        public SetSRandMemberCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SRANDMEMBER";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            if (args.length >= 2) {
                long count = Long.parseLong(args[1]);
                String[] members = dataStore.sRandMember(key, count);
                return RedisMessageHelper.array(Arrays.stream(members)
                        .map(RedisMessageHelper::bulkString)
                        .toList());
            } else {
                String member = dataStore.sRandMember(key);
                return RedisMessageHelper.bulkString(member);
            }
        }
    }
}
