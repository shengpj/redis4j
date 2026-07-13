package com.redis4j.client;

import org.jline.reader.*;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis 命令行客户端（JLine）
 */
public class RedisClientDemo {

    private static final Logger logger = LoggerFactory.getLogger(RedisClientDemo.class);
    private static final String PROMPT = "redis4j> ";

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6666;

        RedisClient client = new RedisClient(host, port);
        RedisCommands commands = new RedisCommands(client);

        Path historyFile = Path.of(System.getProperty("user.home"), ".redis4j_history");
        DefaultHistory history = new DefaultHistory();

        try (Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .jna(true)
                .build()) {
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile.toString())
                    .build();

            try {
                client.connect();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Connection interrupted");
                return;
            }
            if (!client.isConnected()) {
                logger.error("Failed to connect to Redis server at {}:{}", host, port);
                return;
            }
            terminal.writer().println("Connected to Redis4J server at " + host + ":" + port);

            String lastInput = null;
            while (true) {
                lastInput = null;
                try {
                    lastInput = reader.readLine(PROMPT);
                } catch (UserInterruptException e) {
                    continue;
                } catch (EndOfFileException e) {
                    terminal.writer().println();
                    logger.info("Goodbye!");
                    break;
                }

                if (lastInput == null || lastInput.trim().isEmpty()) {
                    continue;
                }

                String line = lastInput.trim();

                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    logger.info("Goodbye!");
                    break;
                }

                if (line.equalsIgnoreCase("help")) {
                    printHelp();
                    continue;
                }

                try {
                    String result = executeCommand(commands, line);
                    if (result != null && !result.isEmpty()) {
                        terminal.writer().println(result);
                    }
                } catch (Exception e) {
                    terminal.writer().println("(error) " + e.getMessage());
                }
            }

        } catch (IOException e) {
            logger.error("Error: {}", e.getMessage(), e);
        } finally {
            try {
                history.write(historyFile, true);
            } catch (IOException ignored) {}
            client.disconnect();
        }
    }

    private static String executeCommand(RedisCommands commands, String line) throws Exception {
        String[] parts = tokenize(line);
        if (parts.length == 0) {
            return "";
        }

        String cmd = parts[0].toUpperCase();
        String[] args = java.util.Arrays.copyOfRange(parts, 1, parts.length);

        return switch (cmd) {
            case "PING" -> commands.ping();
            case "GET" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                String result = commands.get(args[0]);
                yield result != null ? result : "(nil)";
            }
            case "SET" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                commands.set(args[0], args[1]);
                yield "OK";
            }
            case "SETEX" -> {
                if (args.length < 3) throw new IllegalArgumentException("wrong number of arguments");
                commands.setEx(args[0], args[1], Long.parseLong(args[2]));
                yield "OK";
            }
            case "DEL" -> String.valueOf(commands.del(args));
            case "EXISTS" -> String.valueOf(commands.exists(args[0]));
            case "EXPIRE" -> String.valueOf(commands.expire(args[0], Long.parseLong(args[1])));
            case "TTL" -> String.valueOf(commands.ttl(args[0]));
            case "DBSIZE" -> String.valueOf(commands.dbSize());
            case "FLUSHDB" -> {
                commands.flushDb();
                yield "OK";
            }
            case "FLUSHALL" -> {
                commands.flushAll();
                yield "OK";
            }
            case "KEYS" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                var keys = commands.keys(args[0]);
                yield keys.isEmpty() ? "(empty list)" : String.join(" ", keys);
            }
            case "SCAN" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                yield formatScan(commands.scan(args[0], java.util.Arrays.copyOfRange(args, 1, args.length)));
            }
            case "INCR" -> String.valueOf(commands.incr(args[0]));
            case "DECR" -> String.valueOf(commands.decr(args[0]));
            case "INCRBY" -> String.valueOf(commands.incrBy(args[0], Long.parseLong(args[1])));
            case "DECRBY" -> String.valueOf(commands.decrBy(args[0], Long.parseLong(args[1])));
            case "STRLEN" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.strlen(args[0]));
            }
            case "APPEND" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.append(args[0], args[1]));
            }
            case "SETNX" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.setNx(args[0], args[1]));
            }
            case "MGET" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                String[] values = commands.mGet(args);
                if (values.length == 0) yield "(nil)";
                yield String.join(" ", values);
            }
            case "MSET" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                Map<String, String> map = new LinkedHashMap<>();
                for (int i = 0; i < args.length; i += 2) {
                    if (i + 1 < args.length) map.put(args[i], args[i + 1]);
                }
                commands.mSet(map);
                yield "OK";
            }
            case "LPUSH", "RPUSH" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                long count = cmd.equals("LPUSH") ? commands.lPush(args[0], java.util.Arrays.copyOfRange(args, 1, args.length))
                        : commands.rPush(args[0], java.util.Arrays.copyOfRange(args, 1, args.length));
                yield String.valueOf(count);
            }
            case "LPOP", "RPOP" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                String result = cmd.equals("LPOP") ? commands.lPop(args[0]) : commands.rPop(args[0]);
                yield result != null ? result : "(nil)";
            }
            case "LRANGE" -> {
                if (args.length < 3) throw new IllegalArgumentException("wrong number of arguments");
                String[] items = commands.lRange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
                yield items.length == 0 ? "(empty list)" : "[" + String.join(", ", items) + "]";
            }
            case "LLEN" -> String.valueOf(commands.lLen(args[0]));
            case "HSET" -> {
                if (args.length < 3) throw new IllegalArgumentException("wrong number of arguments");
                long count = 0;
                for (int i = 1; i < args.length; i += 2) {
                    if (i + 1 < args.length) {
                        commands.hSet(args[0], args[i], args[i + 1]);
                        count++;
                    }
                }
                yield String.valueOf(count);
            }
            case "HSETNX" -> {
                if (args.length < 3) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.hSetNx(args[0], args[1], args[2]));
            }
            case "HGET" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                String result = commands.hGet(args[0], args[1]);
                yield result != null ? result : "(nil)";
            }
            case "HGETALL" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                var map = commands.hGetAll(args[0]);
                if (map.isEmpty()) {
                    yield "(empty hash)";
                }
                StringBuilder sb = new StringBuilder();
                map.forEach((k, v) -> sb.append(k).append("=").append(v).append(" "));
                yield sb.toString().trim();
            }
            case "HDEL" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                String key = args[0];
                String[] fields = java.util.Arrays.copyOfRange(args, 1, args.length);
                long n = commands.hDel(key, fields);
                yield Long.toString(n);
            }
            case "HEXISTS" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.hExists(args[0], args[1]));
            }
            case "HLEN" -> String.valueOf(commands.hLen(args[0]));
            case "HSCAN" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield formatScan(commands.hScan(args[0], args[1],
                        java.util.Arrays.copyOfRange(args, 2, args.length)));
            }
            case "SADD" -> String.valueOf(commands.sAdd(args[0], java.util.Arrays.copyOfRange(args, 1, args.length)));
            case "SREM" -> String.valueOf(commands.sRem(args[0], java.util.Arrays.copyOfRange(args, 1, args.length)));
            case "SMEMBERS" -> {
                var members = commands.sMembers(args[0]);
                yield members.isEmpty() ? "(empty set)" : String.join(", ", members);
            }
            case "SISMEMBER" -> String.valueOf(commands.sIsMember(args[0], args[1]));
            case "SCARD" -> String.valueOf(commands.sCard(args[0]));
            case "SSCAN" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield formatScan(commands.sScan(args[0], args[1],
                        java.util.Arrays.copyOfRange(args, 2, args.length)));
            }
            case "ZADD" -> {
                if (args.length < 3 || args.length % 2 == 0)
                    throw new IllegalArgumentException("wrong number of arguments");
                java.util.Map<String, Double> members = new java.util.LinkedHashMap<>();
                for (int i = 1; i < args.length; i += 2) {
                    members.put(args[i + 1], Double.parseDouble(args[i]));
                }
                yield String.valueOf(commands.zAdd(args[0], members));
            }
            case "ZREM" -> String.valueOf(commands.zRem(args[0],
                    java.util.Arrays.copyOfRange(args, 1, args.length)));
            case "ZSCORE" -> {
                Double score = commands.zScore(args[0], args[1]);
                yield score == null ? "(nil)" : String.valueOf(score);
            }
            case "ZCARD" -> String.valueOf(commands.zCard(args[0]));
            case "ZINCRBY" -> String.valueOf(commands.zIncrBy(args[0], Double.parseDouble(args[1]), args[2]));
            case "ZRANGE" -> {
                boolean withScores = args.length == 4 && "WITHSCORES".equalsIgnoreCase(args[3]);
                if (args.length > 4 || args.length == 4 && !withScores)
                    throw new IllegalArgumentException("syntax error");
                if (withScores) {
                    var values = commands.zRangeWithScores(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
                    yield formatValues(values.stream()
                            .flatMap(value -> java.util.stream.Stream.of(value.member(), String.valueOf(value.score())))
                            .toArray(String[]::new));
                }
                yield formatValues(commands.zRange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]))
                        .toArray(String[]::new));
            }
            case "ZREVRANGE" -> {
                boolean withScores = args.length == 4 && "WITHSCORES".equalsIgnoreCase(args[3]);
                if (args.length > 4 || args.length == 4 && !withScores)
                    throw new IllegalArgumentException("syntax error");
                if (withScores) {
                    var values = commands.zRevRangeWithScores(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
                    yield formatValues(values.stream()
                            .flatMap(value -> java.util.stream.Stream.of(value.member(), String.valueOf(value.score())))
                            .toArray(String[]::new));
                }
                yield formatValues(commands.zRevRange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]))
                        .toArray(String[]::new));
            }
            case "ZRANK" -> {
                Long rank = commands.zRank(args[0], args[1]);
                yield rank == null ? "(nil)" : String.valueOf(rank);
            }
            case "ZREVRANK" -> {
                Long rank = commands.zRevRank(args[0], args[1]);
                yield rank == null ? "(nil)" : String.valueOf(rank);
            }
            case "ZCOUNT" -> String.valueOf(commands.zCount(args[0], args[1], args[2]));
            case "ZRANGEBYSCORE" -> formatValues(commands.zRangeByScore(args[0], args[1], args[2],
                    java.util.Arrays.copyOfRange(args, 3, args.length)));
            case "ZSCAN" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield formatScan(commands.zScan(args[0], args[1],
                        java.util.Arrays.copyOfRange(args, 2, args.length)));
            }
            case "TYPE" -> {
                String type = commands.type(args[0]);
                yield type != null ? type : "none";
            }
            case "RENAME" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                commands.rename(args[0], args[1]);
                yield "OK";
            }
            case "PERSIST" -> String.valueOf(commands.persist(args[0]));
            case "EXPIREAT" -> {
                if (args.length < 2) throw new IllegalArgumentException("wrong number of arguments");
                yield String.valueOf(commands.expireAt(args[0], Long.parseLong(args[1])));
            }
            case "PTTL" -> String.valueOf(commands.pttl(args[0]));
            case "INFO" -> {
                if (args.length < 1) throw new IllegalArgumentException("INFO requires a section");
                yield commands.info(args[0]);
            }
            case "SAVE" -> {
                commands.save();
                yield "OK";
            }
            case "BGSAVE" -> {
                commands.bgSave();
                yield "Background saving started";
            }
            case "LASTSAVE" -> String.valueOf(commands.lastSave());
            case "SELECT" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                commands.select(Integer.parseInt(args[0]));
                yield "OK";
            }
            case "ECHO" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                yield commands.echo(args[0]);
            }
            case "TIME" -> commands.time();
            default -> "(error) unknown command '" + cmd + "'";
        };
    }

    /**
     * 简单的命令行 token 化，支持双引号内的空格
     */
    private static String[] tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if ((c == '"' || c == '\'') && !inQuote) {
                inQuote = true;
            } else if (inQuote && c == (inQuote ? c : 0)) {
                inQuote = false;
            } else if (!inQuote && Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens.toArray(new String[0]);
    }

    private static String formatScan(RedisCommands.ScanResult result) {
        StringBuilder output = new StringBuilder("1) \"").append(result.cursor()).append("\"\n2)");
        String[] values = result.values();
        if (values.length == 0) return output.append(" (empty array)").toString();
        for (int i = 0; i < values.length; i++) {
            output.append("\n   ").append(i + 1).append(") \"").append(values[i]).append('"');
        }
        return output.toString();
    }

    private static String formatValues(String[] values) {
        if (values.length == 0) return "(empty array)";
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) output.append('\n');
            output.append(i + 1).append(") \"").append(values[i]).append('"');
        }
        return output.toString();
    }

    private static void printHelp() {
        System.out.println("""
            Supported commands:
              String:  PING, GET, SET, SETEX, DEL, EXISTS, EXPIRE, TTL, INCR, DECR, INCRBY, DECRBY, STRLEN, APPEND, SETNX, MGET, MSET
              Key:     KEYS, SCAN, TYPE, DBSIZE, FLUSHDB, FLUSHALL, RENAME, PERSIST, EXPIREAT, PTTL
              List:    LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
              Hash:    HSET, HSETNX, HGET, HGETALL, HDEL, HEXISTS, HLEN, HSCAN
              Set:     SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SSCAN
              ZSet:    ZADD, ZREM, ZSCORE, ZCARD, ZINCRBY, ZRANGE, ZREVRANGE, ZRANK, ZREVRANK, ZCOUNT, ZRANGEBYSCORE, ZSCAN
              Server:  SELECT, ECHO, INFO MEMORY, SAVE, BGSAVE, LASTSAVE, TIME
              Other:   exit, quit, help
            """);
    }
}
