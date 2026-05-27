package com.redis4j.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Redis 命令行客户端
 */
public class RedisClientDemo {

    private static final Logger logger = LoggerFactory.getLogger(RedisClientDemo.class);
    private static final String PROMPT = "redis4j> ";

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6666;

        RedisClient client = new RedisClient(host, port);
        RedisCommands commands = new RedisCommands(client);

        try {
            client.connect();
            if (!client.isConnected()) {
                logger.error("Failed to connect to Redis server at {}:{}", host, port);
                return;
            }
            logger.info("Connected to Redis4J server at {}:{}", host, port);
            logger.info("Type 'exit' to quit, 'help' for command format");

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.print(PROMPT);
                String line = scanner.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

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
                    System.out.println(result);
                } catch (Exception e) {
                    System.out.println("(error) " + e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Error: {}", e.getMessage(), e);
        } finally {
            client.disconnect();
        }
    }

    private static String executeCommand(RedisCommands commands, String line) throws Exception {
        String[] parts = parseCommand(line);
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
            case "KEYS" -> {
                if (args.length < 1) throw new IllegalArgumentException("wrong number of arguments");
                var keys = commands.keys(args[0]);
                yield keys.isEmpty() ? "(empty list)" : String.join(" ", keys);
            }
            case "INCR" -> String.valueOf(commands.incr(args[0]));
            case "DECR" -> String.valueOf(commands.decr(args[0]));
            case "INCRBY" -> String.valueOf(commands.incrBy(args[0], Long.parseLong(args[1])));
            case "DECRBY" -> String.valueOf(commands.decrBy(args[0], Long.parseLong(args[1])));
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
                for (int i = 2; i < args.length; i += 2) {
                    if (i + 1 < args.length) {
                        commands.hSet(args[0], args[i], args[i + 1]);
                        count++;
                    }
                }
                yield String.valueOf(count);
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
            case "SADD" -> String.valueOf(commands.sAdd(args[0], java.util.Arrays.copyOfRange(args, 1, args.length)));
            case "SMEMBERS" -> {
                var members = commands.sMembers(args[0]);
                yield members.isEmpty() ? "(empty set)" : String.join(", ", members);
            }
            case "SISMEMBER" -> String.valueOf(commands.sIsMember(args[0], args[1]));
            case "SCARD" -> String.valueOf(commands.sCard(args[0]));
            case "TYPE" -> {
                String type = commands.type(args[0]);
                yield type != null ? type : "none";
            }
            case "INFO" -> "Redis4J 1.0.0";
            case "SAVE" -> {
                commands.save();
                yield "OK";
            }
            case "BGSAVE" -> {
                commands.bgSave();
                yield "Background saving started";
            }
            case "LASTSAVE" -> String.valueOf(commands.lastSave());
            default -> "(error) unknown command '" + cmd + "'";
        };
    }

    private static String[] parseCommand(String line) {
        java.util.List<String> tokens = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        char quoteChar = 0;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if ((c == '"' || c == '\'') && !inQuote) {
                inQuote = true;
                quoteChar = c;
            } else if (inQuote && c == quoteChar) {
                inQuote = false;
            } else if (!inQuote && c == ' ') {
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

    private static void printHelp() {
        System.out.println("""
            Supported commands:
              String:  PING, GET, SET, SETEX, DEL, EXISTS, EXPIRE, TTL, INCR, DECR, INCRBY, DECRBY
              List:    LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
              Hash:    HSET, HGET, HGETALL
              Set:     SADD, SMEMBERS, SISMEMBER, SCARD
              Server:  DBSIZE, FLUSHDB, KEYS, TYPE, INFO
              Other:   exit, quit, help
            """);
    }
}
