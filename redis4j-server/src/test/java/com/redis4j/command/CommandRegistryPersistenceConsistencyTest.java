package com.redis4j.command;

import com.redis4j.persistence.aof.AofFlushPolicy;
import com.redis4j.persistence.aof.AofManager;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.memory.EvictionPolicy;
import com.redis4j.storage.memory.MemoryLimitManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CommandRegistryPersistenceConsistencyTest {
    @TempDir
    Path tempDir;

    @Test
    void rollsBackWhenAppendThrowsSynchronously() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) -> {
                throw new IOException("disk unavailable");
            });

            CommandResponse response = registry.execute("SET", new String[]{"key", "value"});

            assertInstanceOf(CommandResponse.Error.class, response);
            assertNull(store.get("key"));
        }
    }

    @Test
    void writerStartupFailureCannotLeaveAnEnqueuedCommandWaitingForever() throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve("aof-is-a-directory"));
        try (MemoryStore store = new MemoryStore();
             AofManager aof = new AofManager(directory, AofFlushPolicy.ALWAYS, 16)) {
            aof.start();
            CommandRegistry registry = new CommandRegistry(store);
            registry.setCommandJournal(aof);

            CommandResponse response = assertTimeoutPreemptively(Duration.ofSeconds(2),
                    () -> registry.execute("SET", new String[]{"key", "value"}));

            assertInstanceOf(CommandResponse.Error.class, response);
            assertNull(store.get("key"));
        }
    }

    @Test
    void restoresPreviousValueWhenAsyncAppendFails() throws Exception {
        try (MemoryStore store = new MemoryStore()) {
            store.set("key", "original");
            CompletableFuture<Void> completion = new CompletableFuture<>();
            CountDownLatch appended = new CountDownLatch(1);
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) -> {
                appended.countDown();
                return completion;
            });
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<CommandResponse> result = executor.submit(
                        () -> registry.execute("SET", new String[]{"key", "changed"}));
                assertTrue(appended.await(2, TimeUnit.SECONDS));
                completion.completeExceptionally(new IOException("write failed"));

                assertInstanceOf(CommandResponse.Error.class, result.get(2, TimeUnit.SECONDS));
                assertEquals("original", store.get("key"));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void keepsLaterWriteBehindFailedWriteUntilRollbackCompletes() throws Exception {
        try (MemoryStore store = new MemoryStore()) {
            store.set("key", "original");
            CompletableFuture<Void> firstCompletion = new CompletableFuture<>();
            CompletableFuture<Void> secondCompletion = CompletableFuture.completedFuture(null);
            CountDownLatch firstAppended = new CountDownLatch(1);
            CountDownLatch secondAppended = new CountDownLatch(1);
            AtomicInteger calls = new AtomicInteger();
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) -> {
                if (calls.getAndIncrement() == 0) {
                    firstAppended.countDown();
                    return firstCompletion;
                }
                secondAppended.countDown();
                return secondCompletion;
            });
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<CommandResponse> first = executor.submit(
                        () -> registry.execute("SET", new String[]{"key", "first"}));
                assertTrue(firstAppended.await(2, TimeUnit.SECONDS));
                Future<CommandResponse> second = executor.submit(
                        () -> registry.execute("SET", new String[]{"key", "second"}));

                assertThrows(TimeoutException.class, () -> second.get(150, TimeUnit.MILLISECONDS));
                assertEquals(1, secondAppended.getCount());
                firstCompletion.completeExceptionally(new IOException("first write failed"));

                assertInstanceOf(CommandResponse.Error.class, first.get(2, TimeUnit.SECONDS));
                assertFalse(second.get(2, TimeUnit.SECONDS) instanceof CommandResponse.Error);
                assertEquals("second", store.get("key"));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void restoresAllKeysWhenFlushPersistenceFails() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("first", "1");
            store.rPush("second", "a", "b");
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) ->
                    CompletableFuture.failedFuture(new IOException("write failed")));

            CommandResponse response = registry.execute("FLUSHDB", new String[0]);

            assertInstanceOf(CommandResponse.Error.class, response);
            assertEquals("1", store.get("first"));
            assertArrayEquals(new String[]{"a", "b"}, store.lRange("second", 0, -1));
        }
    }

    @Test
    void restoresCommandAndEvictedKeysWhenPersistenceFails() {
        long limit;
        try (MemoryStore sizing = new MemoryStore()) {
            sizing.set("sample", "x".repeat(200));
            limit = sizing.estimatedMemoryUsage() * 2 + 128;
        }
        try (MemoryStore store = new MemoryStore()) {
            store.set("old", "a".repeat(200));
            store.set("recent", "b".repeat(200));
            AtomicReference<List<String>> evicted = new AtomicReference<>(List.of());
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) -> {
                evicted.set(evictedKeys);
                return CompletableFuture.failedFuture(new IOException("write failed"));
            });
            registry.setMemoryLimitManager(new MemoryLimitManager(store, limit, EvictionPolicy.ALLKEYS_LRU));
            registry.execute("GET", new String[]{"recent"});

            CommandResponse response = registry.execute("SET", new String[]{"new", "c".repeat(200)});

            assertInstanceOf(CommandResponse.Error.class, response);
            assertFalse(evicted.get().isEmpty());
            assertEquals("a".repeat(200), store.get("old"));
            assertEquals("b".repeat(200), store.get("recent"));
            assertNull(store.get("new"));
        }
    }

    @Test
    void interruptionDoesNotRollbackACommandThatEventuallyPersists() throws Exception {
        try (MemoryStore store = new MemoryStore()) {
            CompletableFuture<Void> completion = new CompletableFuture<>();
            CountDownLatch appended = new CountDownLatch(1);
            CommandRegistry registry = registry(store, (name, args, response, evictedKeys) -> {
                appended.countDown();
                return completion;
            });
            AtomicReference<CommandResponse> response = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread writer = new Thread(() -> {
                response.set(registry.execute("SET", new String[]{"key", "value"}));
                interrupted.set(Thread.currentThread().isInterrupted());
            });
            writer.start();
            assertTrue(appended.await(2, TimeUnit.SECONDS));

            writer.interrupt();
            completion.complete(null);
            writer.join(2_000);

            assertFalse(writer.isAlive());
            assertFalse(response.get() instanceof CommandResponse.Error);
            assertTrue(interrupted.get());
            assertEquals("value", store.get("key"));
        }
    }

    private static CommandRegistry registry(MemoryStore store, AppendBehavior appendBehavior) {
        CommandRegistry registry = new CommandRegistry(store);
        registry.setCommandJournal(new CommandJournal() {
            @Override
            public boolean isWriteCommand(String commandName) {
                return WriteCommandSupport.isWriteCommand(commandName);
            }

            @Override
            public CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response)
                    throws IOException {
                return appendBehavior.append(commandName, args, response, List.of());
            }

            @Override
            public CompletableFuture<Void> appendWithEvictions(String commandName, String[] args,
                                                                 CommandResponse response,
                                                                 List<String> evictedKeys) throws IOException {
                return appendBehavior.append(commandName, args, response, evictedKeys);
            }
        });
        return registry;
    }

    @FunctionalInterface
    private interface AppendBehavior {
        CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response,
                                       List<String> evictedKeys) throws IOException;
    }
}
