package com.redis4j.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能基准测试
 * 支持多线程并发测试 SET 和 GET 命令
 */
public class BenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTest.class);
    private static final int OPERATIONS = 100_000;
    private static final int KEY_LENGTH = 20;
    private static final int VALUE_LENGTH = 50;
    private static final int DEFAULT_THREADS = 10;

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom random = new SecureRandom();

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6666;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_THREADS;

        logger.info("==============================================");
        logger.info("        Redis4J 性能基准测试");
        logger.info("==============================================");
        logger.info("服务器: {}:{}", host, port);
        logger.info("并发线程数: {}", threads);
        logger.info("每线程操作数: {}", OPERATIONS / threads);
        logger.info("总操作数: {} 次", OPERATIONS);
        logger.info("Key 长度: {} 字符", KEY_LENGTH);
        logger.info("Value 长度: {} 字符", VALUE_LENGTH);
        logger.info("==============================================");

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        try {
            // 生成测试数据
            List<String> keys = generateKeys(OPERATIONS, KEY_LENGTH);
            List<String> values = generateValues(OPERATIONS, VALUE_LENGTH);

            // 预热 - 单线程
            logger.info("预热中 (单线程)...");
            RedisClient warmupClient = new RedisClient(host, port);
            warmupClient.connect();
            RedisCommands warmupCommands = new RedisCommands(warmupClient);
            warmUp(warmupCommands);
            warmupClient.disconnect();
            logger.info("预热完成\n");

            // 创建客户端连接池
            List<RedisClient> clients = new ArrayList<>(threads);
            List<RedisCommands> commandsList = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                RedisClient client = new RedisClient(host, port);
                client.connect();
                clients.add(client);
                commandsList.add(new RedisCommands(client));
            }

            // 清空数据库
            commandsList.get(0).flushDb();
            logger.info("数据库已清空\n");

            // 测试 SET
            logger.info("==============================================");
            logger.info("开始测试 SET 命令 ({} 线程)...", threads);
            logger.info("==============================================");

            AtomicLong setSuccess = new AtomicLong(0);
            AtomicLong setError = new AtomicLong(0);
            long setStartNano = System.nanoTime();

            CountDownLatch setLatch = new CountDownLatch(threads);
            int opsPerThread = OPERATIONS / threads;

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                final RedisCommands cmd = commandsList.get(t);
                final int start = threadId * opsPerThread;
                final int end = start + opsPerThread;

                executor.submit(() -> {
                    try {
                        for (int i = start; i < end; i++) {
                            try {
                                cmd.set(keys.get(i), values.get(i));
                                setSuccess.incrementAndGet();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                setError.incrementAndGet();
                            }
                        }
                    } finally {
                        setLatch.countDown();
                    }
                });
            }

            setLatch.await();
            long setEndNano = System.nanoTime();
            long setDurationMs = (setEndNano - setStartNano) / 1_000_000;
            double setDurationSec = setDurationMs / 1000.0;
            double setOpsPerSec = OPERATIONS / setDurationSec;
            double setAvgLatencyUs = (setEndNano - setStartNano) / 1000.0 / OPERATIONS;

            logger.info("SET 测试完成!");
            logger.info("  总耗时: {} ms ({} 秒)", setDurationMs, String.format("%.3f", setDurationSec));
            logger.info("  成功操作: {}", setSuccess.get());
            logger.info("  失败操作: {}", setError.get());
            logger.info("  QPS: {} ops/s", String.format("%.2f", setOpsPerSec));
            logger.info("  平均延迟: {} us/操作", String.format("%.2f", setAvgLatencyUs));
            logger.info("");

            // 测试 GET
            logger.info("==============================================");
            logger.info("开始测试 GET 命令 ({} 线程)...", threads);
            logger.info("==============================================");

            AtomicLong getSuccess = new AtomicLong(0);
            AtomicLong getNull = new AtomicLong(0);
            AtomicLong getError = new AtomicLong(0);
            long getStartNano = System.nanoTime();

            CountDownLatch getLatch = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                final RedisCommands cmd = commandsList.get(t);
                final int start = threadId * opsPerThread;
                final int end = start + opsPerThread;

                executor.submit(() -> {
                    try {
                        for (int i = start; i < end; i++) {
                            try {
                                String result = cmd.get(keys.get(i));
                                if (result == null) {
                                    getNull.incrementAndGet();
                                } else {
                                    getSuccess.incrementAndGet();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                getError.incrementAndGet();
                            }
                        }
                    } finally {
                        getLatch.countDown();
                    }
                });
            }

            getLatch.await();
            long getEndNano = System.nanoTime();
            long getDurationMs = (getEndNano - getStartNano) / 1_000_000;
            double getDurationSec = getDurationMs / 1000.0;
            double getOpsPerSec = OPERATIONS / getDurationSec;
            double getAvgLatencyUs = (getEndNano - getStartNano) / 1000.0 / OPERATIONS;

            logger.info("GET 测试完成!");
            logger.info("  总耗时: {} ms ({} 秒)", getDurationMs, String.format("%.3f", getDurationSec));
            logger.info("  成功读取: {}", getSuccess.get());
            logger.info("  返回 null: {}", getNull.get());
            logger.info("  失败操作: {}", getError.get());
            logger.info("  QPS: {} ops/s", String.format("%.2f", getOpsPerSec));
            logger.info("  平均延迟: {} us/操作", String.format("%.2f", getAvgLatencyUs));
            logger.info("");

            // 总结
            logger.info("==============================================");
            logger.info("               测试总结");
            logger.info("==============================================");
            logger.info("并发线程: {}", threads);
            logger.info("SET 性能: {} ms | {} ops/s | {} us/op",
                    setDurationMs, String.format("%.2f", setOpsPerSec), String.format("%.2f", setAvgLatencyUs));
            logger.info("GET 性能: {} ms | {} ops/s | {} us/op",
                    getDurationMs, String.format("%.2f", getOpsPerSec), String.format("%.2f", getAvgLatencyUs));
            logger.info("==============================================");

            // 清理
            commandsList.get(0).flushDb();

            // 关闭所有连接
            for (RedisClient client : clients) {
                client.disconnect();
            }

        } catch (Exception e) {
            logger.error("测试失败: {}", e.getMessage(), e);
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("\n测试完成");
        }
    }

    private static void warmUp(RedisCommands commands) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            commands.set("warmup_key_" + i, "warmup_value_" + i);
            commands.get("warmup_key_" + i);
        }
        commands.flushDb();
    }

    private static List<String> generateKeys(int count, int length) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(generateRandomString(length));
        }
        return keys;
    }

    private static List<String> generateValues(int count, int length) {
        List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(generateRandomString(length));
        }
        return values;
    }

    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }
}
