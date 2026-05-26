package com.redis4j.command.impl;

import com.redis4j.command.Command;
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

    public static class SetSAddCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'sadd' command");
            }
            String key = args[0];
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sAdd(key, members);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SREM ====================

    public static class SetSRemCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'srem' command");
            }
            String key = args[0];
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sRem(key, members);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SMEMBERS ====================

    public static class SetSMembersCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'smembers' command");
            }
            Set<String> members = dataStore.sMembers(args[0]);
            return RedisMessageHelper.array(members.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SISMEMBER ====================

    public static class SetSIsMemberCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'sismember' command");
            }
            boolean isMember = dataStore.sIsMember(args[0], args[1]);
            return RedisMessageHelper.integer(isMember ? 1 : 0);
        }
    }

    // ==================== SCARD ====================

    public static class SetSCardCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'scard' command");
            }
            long count = dataStore.sCard(args[0]);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== SINTER ====================

    public static class SetSInterCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'sinter' command");
            }
            Set<String> result = dataStore.sInter(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SUNION ====================

    public static class SetSUnionCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'sunion' command");
            }
            Set<String> result = dataStore.sUnion(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SDIFF ====================

    public static class SetSDiffCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'sdiff' command");
            }
            Set<String> result = dataStore.sDiff(args);
            return RedisMessageHelper.array(result.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== SMOVE ====================

    public static class SetSMoveCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'smove' command");
            }
            boolean result = dataStore.sMove(args[0], args[1], args[2]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== SPOP ====================

    public static class SetSPopCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'spop' command");
            }
            String member = dataStore.sPop(args[0]);
            return RedisMessageHelper.bulkString(member);
        }
    }

    // ==================== SRANDMEMBER ====================

    public static class SetSRandMemberCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'srandmember' command");
            }
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
