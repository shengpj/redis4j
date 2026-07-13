package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.ZSetStore;
import com.redis4j.storage.ZSetStore.ScoredMember;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class SortedSetCommands {
    private SortedSetCommands() {}

    @RedisCommand
    public static final class ZAddCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZAddCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZADD"; }
        public int getArity() { return -3; }
        protected boolean validate(String[] args) { return args != null && args.length >= 3 && args.length % 2 == 1; }
        protected CommandResponse doExecute(String[] args) {
            Map<String, Double> members = new LinkedHashMap<>();
            for (int i = 1; i < args.length; i += 2) members.put(args[i + 1], parseScore(args[i]));
            return CommandResponses.integer(store.zAdd(args[0], members));
        }
    }

    @RedisCommand
    public static final class ZRemCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRemCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZREM"; }
        public int getArity() { return -2; }
        protected CommandResponse doExecute(String[] args) {
            return CommandResponses.integer(store.zRem(args[0], Arrays.copyOfRange(args, 1, args.length)));
        }
    }

    @RedisCommand
    public static final class ZScoreCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZScoreCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZSCORE"; }
        public int getArity() { return 3; }
        protected CommandResponse doExecute(String[] args) {
            Double score = store.zScore(args[0], args[1]);
            return CommandResponses.bulkString(score == null ? null : formatScore(score));
        }
    }

    @RedisCommand
    public static final class ZCardCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZCardCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZCARD"; }
        public int getArity() { return 2; }
        protected CommandResponse doExecute(String[] args) { return CommandResponses.integer(store.zCard(args[0])); }
    }

    @RedisCommand
    public static final class ZIncrByCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZIncrByCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZINCRBY"; }
        public int getArity() { return 4; }
        protected CommandResponse doExecute(String[] args) {
            return CommandResponses.bulkString(formatScore(store.zIncrBy(args[0], parseScore(args[1]), args[2])));
        }
    }

    @RedisCommand
    public static final class ZRangeCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRangeCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZRANGE"; }
        public int getArity() { return -3; }
        protected boolean validate(String[] args) { return args != null && (args.length == 3 || args.length == 4); }
        protected CommandResponse doExecute(String[] args) { return rangeByRank(store, args, false); }
    }

    @RedisCommand
    public static final class ZRevRangeCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRevRangeCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZREVRANGE"; }
        public int getArity() { return -3; }
        protected boolean validate(String[] args) { return args != null && (args.length == 3 || args.length == 4); }
        protected CommandResponse doExecute(String[] args) { return rangeByRank(store, args, true); }
    }

    @RedisCommand
    public static final class ZRankCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRankCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZRANK"; }
        public int getArity() { return 3; }
        protected CommandResponse doExecute(String[] args) { return rank(store, args, false); }
    }

    @RedisCommand
    public static final class ZRevRankCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRevRankCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZREVRANK"; }
        public int getArity() { return 3; }
        protected CommandResponse doExecute(String[] args) { return rank(store, args, true); }
    }

    @RedisCommand
    public static final class ZCountCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZCountCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZCOUNT"; }
        public int getArity() { return 4; }
        protected CommandResponse doExecute(String[] args) {
            ScoreBound min = parseBound(args[1]);
            ScoreBound max = parseBound(args[2]);
            return CommandResponses.integer(store.zCount(args[0], min.value(), min.inclusive(),
                    max.value(), max.inclusive()));
        }
    }

    @RedisCommand
    public static final class ZRangeByScoreCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZRangeByScoreCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZRANGEBYSCORE"; }
        public int getArity() { return -3; }
        protected CommandResponse doExecute(String[] args) {
            ScoreBound min = parseBound(args[1]);
            ScoreBound max = parseBound(args[2]);
            boolean withScores = false;
            long offset = 0;
            long count = Long.MAX_VALUE;
            for (int i = 3; i < args.length;) {
                String option = args[i].toUpperCase(Locale.ROOT);
                if ("WITHSCORES".equals(option)) {
                    withScores = true;
                    i++;
                } else if ("LIMIT".equals(option) && i + 2 < args.length) {
                    offset = parseNonNegativeLong(args[i + 1]);
                    count = parseLongValue(args[i + 2]);
                    if (count < 0) count = Long.MAX_VALUE;
                    i += 3;
                } else {
                    throw new IllegalArgumentException("syntax error");
                }
            }
            return scoredResponse(store.zRangeByScore(args[0], min.value(), min.inclusive(),
                    max.value(), max.inclusive(), offset, count), withScores);
        }
    }

    @RedisCommand
    public static final class ZScanCommand extends AbstractCommand {
        private final ZSetStore store;
        public ZScanCommand(ZSetStore store) { this.store = store; }
        public String getName() { return "ZSCAN"; }
        public int getArity() { return -2; }
        protected CommandResponse doExecute(String[] args) {
            ScanSupport.Options options = ScanSupport.parse(args, 1, 2);
            Map<String, Double> scores = store.zGetAll(args[0]);
            List<String> members = new ArrayList<>(scores.keySet());
            Collections.sort(members);
            ScanSupport.Page page = ScanSupport.scan(members, options);
            List<String> values = new ArrayList<>(page.values().size() * 2);
            for (String member : page.values()) {
                values.add(member);
                values.add(formatScore(scores.get(member)));
            }
            return ScanSupport.response(new ScanSupport.Page(page.nextCursor(), values));
        }
    }

    private static CommandResponse rangeByRank(ZSetStore store, String[] args, boolean reverse) {
        boolean withScores = args.length == 4 && "WITHSCORES".equalsIgnoreCase(args[3]);
        if (args.length == 4 && !withScores) throw new IllegalArgumentException("syntax error");
        return scoredResponse(store.zRange(args[0], parseLongValue(args[1]), parseLongValue(args[2]), reverse),
                withScores);
    }

    private static CommandResponse rank(ZSetStore store, String[] args, boolean reverse) {
        Long value = store.zRank(args[0], args[1], reverse);
        return value == null ? CommandResponses.bulkString((String) null) : CommandResponses.integer(value);
    }

    private static CommandResponse scoredResponse(List<ScoredMember> values, boolean withScores) {
        List<CommandResponse> response = new ArrayList<>(values.size() * (withScores ? 2 : 1));
        for (ScoredMember value : values) {
            response.add(CommandResponses.bulkString(value.member()));
            if (withScores) response.add(CommandResponses.bulkString(formatScore(value.score())));
        }
        return CommandResponses.array(response);
    }

    private static double parseScore(String value) {
        try {
            double score = Double.parseDouble(value);
            if (Double.isNaN(score)) throw new NumberFormatException();
            return score == 0.0 ? 0.0 : score;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not a valid float");
        }
    }

    private static ScoreBound parseBound(String value) {
        boolean inclusive = !value.startsWith("(");
        String number = inclusive ? value : value.substring(1);
        if (number.isEmpty()) throw new IllegalArgumentException("min or max is not a float");
        try {
            double parsed = switch (number.toLowerCase(Locale.ROOT)) {
                case "+inf", "inf" -> Double.POSITIVE_INFINITY;
                case "-inf" -> Double.NEGATIVE_INFINITY;
                default -> Double.parseDouble(number);
            };
            if (Double.isNaN(parsed)) throw new NumberFormatException();
            return new ScoreBound(parsed, inclusive);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("min or max is not a float");
        }
    }

    private static long parseNonNegativeLong(String value) {
        long parsed = parseLongValue(value);
        if (parsed < 0) throw new IllegalArgumentException("offset is out of range");
        return parsed;
    }

    private static long parseLongValue(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
    }

    static String formatScore(double score) {
        if (score == Double.POSITIVE_INFINITY) return "inf";
        if (score == Double.NEGATIVE_INFINITY) return "-inf";
        String value = Double.toString(score).replace("E", "e");
        int exponent = value.indexOf('e');
        if (exponent >= 0 && value.substring(0, exponent).endsWith(".0")) {
            return value.substring(0, exponent - 2) + value.substring(exponent);
        }
        return value.endsWith(".0") ? value.substring(0, value.length() - 2) : value;
    }

    private record ScoreBound(double value, boolean inclusive) {}
}
