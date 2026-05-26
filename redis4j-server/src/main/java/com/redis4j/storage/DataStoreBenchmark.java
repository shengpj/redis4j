package com.redis4j.storage;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DataStore 实现性能基准测试
 * 支持单线程和多线程并发测试
 */
public class DataStoreBenchmark {

    private static final int WARMUP_OPS = 100_000;
    private static final int BENCHMARK_OPS = 1_000_000;
    private static final int KEY_LENGTH = 20;
    private static final int VALUE_LENGTH = 50;
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8, 16};

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom random = new SecureRandom();

    public static void main(String[] args) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        System.out.println("================================================================================");
        System.out.println("                    DataStore 性能基准测试");
        System.out.println("================================================================================");
        System.out.println("系统信息:");
        System.out.println("  CPU 核心数: " + cpuCores);
        System.out.println("测试配置:");
        System.out.println("  预热操作次数: " + WARMUP_OPS);
        System.out.println("  基准测试操作次数: " + BENCHMARK_OPS);
        System.out.println("  Key 长度: " + KEY_LENGTH + " 字符");
        System.out.println("  Value 长度: " + VALUE_LENGTH + " 字符");
        System.out.println("  并发线程数: " + java.util.Arrays.toString(THREAD_COUNTS));
        System.out.println("================================================================================\n");

        // 生成测试数据
        System.out.println("生成测试数据...");
        List<String> keys = generateRandomStrings(BENCHMARK_OPS, KEY_LENGTH);
        List<String> values = generateRandomStrings(BENCHMARK_OPS, VALUE_LENGTH);
        System.out.println("测试数据生成完成\n");

        // =========================================================================
        // 单线程测试
        // =========================================================================
        System.out.println("################################################################################");
        System.out.println("#                           单线程测试");
        System.out.println("################################################################################\n");

        testStoreSingleThread("MemoryStore (ConcurrentHashMap)", new MemoryStore(), keys, values);
        testStoreSingleThread("PartitionedMemoryStore (4 分区)", new PartitionedMemoryStore(4), keys, values);
        testStoreSingleThread("PartitionedMemoryStore (8 分区)", new PartitionedMemoryStore(8), keys, values);
        testStoreSingleThread("PartitionedMemoryStore (16 分区)", new PartitionedMemoryStore(16), keys, values);

        // =========================================================================
        // 多线程并发测试
        // =========================================================================
        System.out.println("\n################################################################################");
        System.out.println("#                           多线程并发测试");
        System.out.println("################################################################################\n");

        for (int threads : THREAD_COUNTS) {
            if (threads == 1) continue; // 跳过单线程，已经测过了
            System.out.println("======= " + threads + " 线程并发 =======");
            testStoreMultiThread("MemoryStore", new MemoryStore(), keys, values, threads);
            testStoreMultiThread("PartitionedMemoryStore (8)", new PartitionedMemoryStore(8), keys, values, threads);
            testStoreMultiThread("PartitionedMemoryStore (16)", new PartitionedMemoryStore(16), keys, values, threads);
            System.out.println();
        }

        // =========================================================================
        // 性能对比总结
        // =========================================================================
        System.out.println("\n################################################################################");
        System.out.println("#                           性能对比总结 (8线程)");
        System.out.println("################################################################################\n");

        compareStores(keys, values, 8);

        System.out.println("\n================================================================================");
        System.out.println("                         测试完成");
        System.out.println("================================================================================");
    }

    private static void testStoreSingleThread(String name, DataStore store, List<String> keys, List<String> values) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("测试: " + name + " (单线程)");
        System.out.println("--------------------------------------------------------------------------------");

        // 预热
        System.out.print("预热中... ");
        for (int i = 0; i < WARMUP_OPS; i++) {
            store.set(keys.get(i), values.get(i));
        }
        for (int i = 0; i < WARMUP_OPS; i++) {
            store.get(keys.get(i));
        }
        store.flushDb();
        System.out.println("完成");

        store.flushDb();

        // 测试 SET
        System.out.print("SET: ");
        long start = System.nanoTime();
        for (int i = 0; i < BENCHMARK_OPS; i++) {
            store.set(keys.get(i), values.get(i));
        }
        long end = System.nanoTime();
        printResult(start, end, BENCHMARK_OPS);

        // 测试 GET
        System.out.print("GET: ");
        start = System.nanoTime();
        for (int i = 0; i < BENCHMARK_OPS; i++) {
            store.get(keys.get(i));
        }
        end = System.nanoTime();
        printResult(start, end, BENCHMARK_OPS);

        store.flushDb();
        store.close();
        System.out.println();
    }

    private static void testStoreMultiThread(String name, DataStore store, List<String> keys, List<String> values, int threadCount) {
        // 预热
        ExecutorService warmupPool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch warmupLatch = new CountDownLatch(threadCount);
        int warmupPerThread = WARMUP_OPS / threadCount;

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            warmupPool.submit(() -> {
                int start = threadId * warmupPerThread;
                int end = start + warmupPerThread;
                for (int i = start; i < end; i++) {
                    store.set(keys.get(i), values.get(i));
                }
                warmupLatch.countDown();
            });
        }

        try {
            warmupLatch.await();
            warmupPool.shutdown();
            warmupPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        store.flushDb();

        // 测试 SET
        System.out.print("SET: ");
        long start = System.nanoTime();
        ExecutorService setPool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch setLatch = new CountDownLatch(threadCount);
        int opsPerThread = BENCHMARK_OPS / threadCount;

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            setPool.submit(() -> {
                int startIdx = threadId * opsPerThread;
                int endIdx = startIdx + opsPerThread;
                for (int i = startIdx; i < endIdx; i++) {
                    store.set(keys.get(i), values.get(i));
                }
                setLatch.countDown();
            });
        }

        try {
            setLatch.await();
            setPool.shutdown();
            setPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long end = System.nanoTime();
        printResult(start, end, BENCHMARK_OPS);

        // 测试 GET
        System.out.print("GET: ");
        start = System.nanoTime();
        ExecutorService getPool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch getLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            getPool.submit(() -> {
                int startIdx = threadId * opsPerThread;
                int endIdx = startIdx + opsPerThread;
                for (int i = startIdx; i < endIdx; i++) {
                    store.get(keys.get(i));
                }
                getLatch.countDown();
            });
        }

        try {
            getLatch.await();
            getPool.shutdown();
            getPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        end = System.nanoTime();
        printResult(start, end, BENCHMARK_OPS);

        store.flushDb();
        store.close();
    }

    private static void compareStores(List<String> keys, List<String> values, int threadCount) {
        System.out.println("8线程并发性能对比:");
        System.out.println("--------------------------------------------------------------------------------");

        int opsPerThread = BENCHMARK_OPS / threadCount;

        // 测试 MemoryStore
        DataStore memoryStore = new MemoryStore();
        warmupMultiThread(memoryStore, keys, values, threadCount, opsPerThread);

        System.out.print("MemoryStore SET: ");
        long start = System.nanoTime();
        runMultiThread(memoryStore, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                memoryStore.set(keys.get(i), values.get(i));
            }
        }, threadCount, opsPerThread);
        long memorySetEnd = System.nanoTime();
        double memorySetOps = printResult(start, memorySetEnd, BENCHMARK_OPS);

        System.out.print("MemoryStore GET: ");
        start = System.nanoTime();
        runMultiThread(memoryStore, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                memoryStore.get(keys.get(i));
            }
        }, threadCount, opsPerThread);
        double memoryGetOps = printResult(start, System.nanoTime(), BENCHMARK_OPS);

        memoryStore.flushDb();
        memoryStore.close();

        // 测试 PartitionedMemoryStore (8)
        DataStore partitioned8 = new PartitionedMemoryStore(8);
        warmupMultiThread(partitioned8, keys, values, threadCount, opsPerThread);

        System.out.print("Partitioned(8) SET: ");
        start = System.nanoTime();
        runMultiThread(partitioned8, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                partitioned8.set(keys.get(i), values.get(i));
            }
        }, threadCount, opsPerThread);
        double part8SetOps = printResult(start, System.nanoTime(), BENCHMARK_OPS);

        System.out.print("Partitioned(8) GET: ");
        start = System.nanoTime();
        runMultiThread(partitioned8, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                partitioned8.get(keys.get(i));
            }
        }, threadCount, opsPerThread);
        double part8GetOps = printResult(start, System.nanoTime(), BENCHMARK_OPS);

        partitioned8.flushDb();
        partitioned8.close();

        // 测试 PartitionedMemoryStore (16)
        DataStore partitioned16 = new PartitionedMemoryStore(16);
        warmupMultiThread(partitioned16, keys, values, threadCount, opsPerThread);

        System.out.print("Partitioned(16) SET: ");
        start = System.nanoTime();
        runMultiThread(partitioned16, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                partitioned16.set(keys.get(i), values.get(i));
            }
        }, threadCount, opsPerThread);
        double part16SetOps = printResult(start, System.nanoTime(), BENCHMARK_OPS);

        System.out.print("Partitioned(16) GET: ");
        start = System.nanoTime();
        runMultiThread(partitioned16, (startIdx, endIdx) -> {
            for (int i = startIdx; i < endIdx; i++) {
                partitioned16.get(keys.get(i));
            }
        }, threadCount, opsPerThread);
        double part16GetOps = printResult(start, System.nanoTime(), BENCHMARK_OPS);

        partitioned16.flushDb();
        partitioned16.close();

        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("总结:");
        System.out.printf("  SET 性能: MemoryStore=%.2f M/s | Partitioned(8)=%.2f M/s | Partitioned(16)=%.2f M/s%n",
                memorySetOps / 1_000_000, part8SetOps / 1_000_000, part16SetOps / 1_000_000);
        System.out.printf("  GET 性能: MemoryStore=%.2f M/s | Partitioned(8)=%.2f M/s | Partitioned(16)=%.2f M/s%n",
                memoryGetOps / 1_000_000, part8GetOps / 1_000_000, part16GetOps / 1_000_000);
    }

    private static void warmupMultiThread(DataStore store, List<String> keys, List<String> values, int threadCount, int opsPerThread) {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                int start = threadId * opsPerThread;
                int end = start + opsPerThread;
                for (int i = start; i < end; i++) {
                    store.set(keys.get(i), values.get(i));
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        store.flushDb();
    }

    private static void runMultiThread(DataStore store, Operation op, int threadCount, int opsPerThread) {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                int start = threadId * opsPerThread;
                int end = start + opsPerThread;
                op.execute(start, end);
                latch.countDown();
            });
        }

        try {
            latch.await();
            pool.shutdown();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static double printResult(long start, long end, int ops) {
        double durationMs = (end - start) / 1_000_000.0;
        double opsPerSec = ops / (durationMs / 1000.0);
        System.out.printf("%.2f ms | %.2f M/s | %.2f ns/op%n", durationMs, opsPerSec / 1_000_000, (end - start) / (double) ops);
        return opsPerSec;
    }

    @FunctionalInterface
    interface Operation {
        void execute(int start, int end);
    }

    private static List<String> generateRandomStrings(int count, int length) {
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(generateRandomString(length));
        }
        return result;
    }

    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }
}
