package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.SetStore;
import com.redis4j.protocol.response.CommandResponse;

import java.util.Arrays;
import java.util.Set;

/**
 * Set 鍛戒护瀹炵幇
 */
public class SetCommands {

    // ==================== SADD ====================

    @RedisCommand
    public static class SetSAddCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSAddCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SADD";
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
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sAdd(key, members);
            return CommandResponses.integer(count);
        }
    }

    // ==================== SREM ====================

    @RedisCommand
    public static class SetSRemCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSRemCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SREM";
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
            String[] members = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.sRem(key, members);
            return CommandResponses.integer(count);
        }
    }

    // ==================== SMEMBERS ====================

    @RedisCommand
    public static class SetSMembersCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSMembersCommand(SetStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            Set<String> members = dataStore.sMembers(args[0]);
            return CommandResponses.array(members.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== SISMEMBER ====================

    @RedisCommand
    public static class SetSIsMemberCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSIsMemberCommand(SetStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            boolean isMember = dataStore.sIsMember(args[0], args[1]);
            return CommandResponses.integer(isMember ? 1 : 0);
        }
    }

    // ==================== SCARD ====================

    @RedisCommand
    public static class SetSCardCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSCardCommand(SetStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long count = dataStore.sCard(args[0]);
            return CommandResponses.integer(count);
        }
    }

    // ==================== SINTER ====================

    @RedisCommand
    public static class SetSInterCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSInterCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SINTER";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            Set<String> result = dataStore.sInter(args);
            return CommandResponses.array(result.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== SUNION ====================

    @RedisCommand
    public static class SetSUnionCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSUnionCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SUNION";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            Set<String> result = dataStore.sUnion(args);
            return CommandResponses.array(result.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== SDIFF ====================

    @RedisCommand
    public static class SetSDiffCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSDiffCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SDIFF";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            Set<String> result = dataStore.sDiff(args);
            return CommandResponses.array(result.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== SMOVE ====================

    @RedisCommand
    public static class SetSMoveCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSMoveCommand(SetStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            boolean result = dataStore.sMove(args[0], args[1], args[2]);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== SPOP ====================

    @RedisCommand
    public static class SetSPopCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSPopCommand(SetStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String member = dataStore.sPop(args[0]);
            return CommandResponses.bulkString(member);
        }
    }

    // ==================== SRANDMEMBER ====================

    @RedisCommand
    public static class SetSRandMemberCommand extends AbstractCommand {
        private final SetStore dataStore;

        public SetSRandMemberCommand(SetStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SRANDMEMBER";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            if (args.length >= 2) {
                long count = parseLong(args[1]);
                String[] members = dataStore.sRandMember(key, count);
                return CommandResponses.array(Arrays.stream(members)
                        .map(CommandResponses::bulkString)
                        .toList());
            } else {
                String member = dataStore.sRandMember(key);
                return CommandResponses.bulkString(member);
            }
        }
    }
}
