package com.redis4j.server;

import com.redis4j.protocol.RedisMessageHelper;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ServerObservability {
    private final ServerConfig config;
    private final SlowLog slowLog;
    private final ClientRegistry clients = new ClientRegistry();
    private final ConnectionLimiter connectionLimiter;

    ServerObservability(ServerConfig config) {
        this.config = config;
        this.slowLog = new SlowLog(config.getSlowLogSlowerThanMicros(), config.getSlowLogMaxLen());
        this.connectionLimiter = new ConnectionLimiter(config.getMaxClients());
    }

    boolean register(Channel channel) {
        if (!connectionLimiter.tryAcquire()) return false;
        try {
            clients.register(channel);
            return true;
        } catch (RuntimeException e) {
            connectionLimiter.release();
            throw e;
        }
    }

    void remove(Channel channel) {
        clients.remove(channel);
        connectionLimiter.release();
    }

    void commandReceived(Channel channel, String command) {
        clients.touch(channel, command);
    }

    void record(Channel channel, String command, String[] args, long durationMicros) {
        if (isManagementCommand(command)) return;
        slowLog.record(command, args, durationMicros, clients.address(channel), "");
    }

    boolean handles(String command) {
        return isManagementCommand(command);
    }

    RedisMessage execute(PubSubBroker pubSubBroker, String command, String[] args) {
        return switch (command) {
            case "SLOWLOG" -> slowLog(args);
            case "CLIENT" -> clientList(pubSubBroker, args);
            case "CONFIG" -> configGet(args);
            default -> RedisMessageHelper.error("ERR unsupported observability command");
        };
    }

    private RedisMessage slowLog(String[] args) {
        if (args.length == 0) return wrongArguments("slowlog");
        return switch (args[0].toUpperCase(Locale.ROOT)) {
            case "GET" -> {
                if (args.length > 2) yield wrongArguments("slowlog|get");
                int count = args.length == 1 ? 10 : parseCount(args[1]);
                List<RedisMessage> response = new ArrayList<>();
                for (SlowLog.Entry entry : slowLog.get(count)) {
                    List<RedisMessage> command = entry.arguments().stream()
                            .map(RedisMessageHelper::bulkString)
                            .toList();
                    response.add(RedisMessageHelper.array(
                            RedisMessageHelper.integer(entry.id()),
                            RedisMessageHelper.integer(entry.timestampSeconds()),
                            RedisMessageHelper.integer(entry.durationMicros()),
                            RedisMessageHelper.array(command),
                            RedisMessageHelper.bulkString(entry.clientAddress()),
                            RedisMessageHelper.bulkString(entry.clientName())));
                }
                yield RedisMessageHelper.array(response);
            }
            case "LEN" -> args.length == 1
                    ? RedisMessageHelper.integer(slowLog.length()) : wrongArguments("slowlog|len");
            case "RESET" -> {
                if (args.length != 1) yield wrongArguments("slowlog|reset");
                slowLog.reset();
                yield RedisMessageHelper.ok();
            }
            default -> RedisMessageHelper.error("ERR unknown SLOWLOG subcommand '" + args[0] + "'");
        };
    }

    private RedisMessage clientList(PubSubBroker pubSubBroker, String[] args) {
        if (args.length != 1 || !"LIST".equalsIgnoreCase(args[0])) {
            return RedisMessageHelper.error("ERR only CLIENT LIST is supported");
        }
        return RedisMessageHelper.bulkString(clients.list(pubSubBroker));
    }

    private RedisMessage configGet(String[] args) {
        if (args.length != 2 || !"GET".equalsIgnoreCase(args[0])) {
            return RedisMessageHelper.error("ERR only CONFIG GET is supported");
        }
        String pattern = args[1].toLowerCase(Locale.ROOT);
        List<RedisMessage> result = new ArrayList<>();
        for (Map.Entry<String, String> value : config.asConfigMap().entrySet()) {
            if (!globMatches(pattern, value.getKey())) continue;
            result.add(RedisMessageHelper.bulkString(value.getKey()));
            result.add(RedisMessageHelper.bulkString(value.getValue()));
        }
        return RedisMessageHelper.array(result);
    }

    private static int parseCount(String value) {
        try {
            int count = Integer.parseInt(value);
            if (count < 0) throw new NumberFormatException();
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("count must be a non-negative integer");
        }
    }

    private static RedisMessage wrongArguments(String command) {
        return RedisMessageHelper.error("ERR wrong number of arguments for '" + command + "' command");
    }

    private static boolean isManagementCommand(String command) {
        return "SLOWLOG".equals(command) || "CLIENT".equals(command) || "CONFIG".equals(command);
    }

    private static boolean globMatches(String pattern, String value) {
        int patternIndex = 0;
        int valueIndex = 0;
        int starIndex = -1;
        int starMatch = -1;
        while (valueIndex < value.length()) {
            if (patternIndex < pattern.length()
                    && (pattern.charAt(patternIndex) == '?'
                    || pattern.charAt(patternIndex) == value.charAt(valueIndex))) {
                patternIndex++;
                valueIndex++;
            } else if (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
                starIndex = patternIndex++;
                starMatch = valueIndex;
            } else if (starIndex >= 0) {
                patternIndex = starIndex + 1;
                valueIndex = ++starMatch;
            } else {
                return false;
            }
        }
        while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') patternIndex++;
        return patternIndex == pattern.length();
    }

    SlowLog slowLog() {
        return slowLog;
    }

    ClientRegistry clients() {
        return clients;
    }

    ClientConnectionMetrics connectionMetrics() {
        return connectionLimiter.snapshot();
    }
}
