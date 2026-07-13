package com.redis4j.persistence.aof;

import com.redis4j.command.CommandJournal;
import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单写线程 AOF 管理器。
 *
 * <p>业务线程只负责构造记录并进入有界队列，唯一写线程按队列顺序批量写入 FileChannel，
 * 从而避免多线程竞争文件位置，并把随机小写合并为顺序大块写。</p>
 */
public final class AofManager implements CommandJournal, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AofManager.class);
    private static final int WRITE_BUFFER_SIZE = 256 * 1024;
    private static final int MAX_BATCH_SIZE = 1024;
    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "SETNX", "SETEX", "MSET", "INCR", "INCRBY", "DECR", "DECRBY", "APPEND",
            "DEL", "EXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "RENAME", "FLUSHDB", "FLUSHALL",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
            "HSET", "HSETNX", "HDEL", "HMSET", "HINCRBY",
            "SADD", "SREM", "SMOVE", "SPOP");

    private final Path path;
    private final AofFlushPolicy flushPolicy;
    private final ArrayBlockingQueue<AppendTask> queue;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile IOException failure;
    private Thread writerThread;

    public AofManager(Path path, AofFlushPolicy flushPolicy, int queueCapacity) {
        if (queueCapacity <= 0) throw new IllegalArgumentException("AOF queue capacity must be positive");
        this.path = path.toAbsolutePath();
        this.flushPolicy = flushPolicy;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
    }

    public boolean exists() {
        return Files.exists(path);
    }

    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) return;
        Path parent = path.getParent();
        if (parent != null) Files.createDirectories(parent);
        writerThread = new Thread(this::writerLoop, "aof-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    @Override
    public boolean isWriteCommand(String commandName) {
        return WRITE_COMMANDS.contains(commandName.toUpperCase());
    }

    @Override
    public CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response) throws IOException {
        List<AofCommand> commands = canonicalize(commandName, args, response, System.currentTimeMillis());
        return commands.isEmpty() ? CompletableFuture.completedFuture(null) : enqueueCommands(commands, false);
    }

    public void appendSnapshot(DataSnapshot snapshot) throws IOException {
        for (SnapshotEntry entry : snapshot.entries()) {
            List<AofCommand> commands = snapshotCommands(entry);
            if (!commands.isEmpty()) await(enqueueCommands(commands, false));
        }
        await(enqueueForce());
    }

    public int recover(CommandRegistry registry) throws IOException {
        if (!Files.exists(path)) return 0;
        int applied = 0;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long position = 0;
            long lastGoodPosition = 0;
            while (position < channel.size()) {
                ByteBuffer header = ByteBuffer.allocate(AofRecordCodec.HEADER_SIZE);
                int headerBytes = readFully(channel, header, position);
                if (headerBytes < AofRecordCodec.HEADER_SIZE) {
                    truncateTail(channel, lastGoodPosition, "incomplete record header");
                    break;
                }
                header.flip();
                int magic = header.getInt();
                byte version = header.get();
                header.getLong(); // 记录时间戳已在写入时用于 TTL 规范化，恢复时无需再次换算。
                int payloadLength = header.getInt();
                int checksum = header.getInt();
                if (magic != AofRecordCodec.MAGIC || version != AofRecordCodec.VERSION
                        || payloadLength <= 0 || payloadLength > AofRecordCodec.MAX_PAYLOAD_SIZE) {
                    truncateTail(channel, lastGoodPosition, "invalid record header");
                    break;
                }
                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
                int payloadBytes = readFully(channel, payloadBuffer, position + AofRecordCodec.HEADER_SIZE);
                if (payloadBytes < payloadLength) {
                    truncateTail(channel, lastGoodPosition, "incomplete record payload");
                    break;
                }
                byte[] payload = payloadBuffer.array();
                if (!AofRecordCodec.checksumMatches(payload, checksum)) {
                    truncateTail(channel, lastGoodPosition, "checksum mismatch");
                    break;
                }
                for (AofCommand command : AofRecordCodec.decode(payload)) {
                    CommandResponse result = registry.executeReplay(command.name(), command.args());
                    if (result instanceof CommandResponse.Error error) {
                        throw new IOException("Failed to replay AOF command " + command.name() + ": " + error.value());
                    }
                    applied++;
                }
                position += AofRecordCodec.HEADER_SIZE + payloadLength;
                lastGoodPosition = position;
            }
        }
        logger.info("AOF recovery completed: {} commands applied from {}", applied, path);
        return applied;
    }

    private CompletableFuture<Void> enqueueCommands(List<AofCommand> commands, boolean force) throws IOException {
        IOException currentFailure = failure;
        if (currentFailure != null) throw new IOException("AOF writer is unavailable", currentFailure);
        if (!started.get()) throw new IOException("AOF writer has not been started");
        byte[] record = AofRecordCodec.encode(System.currentTimeMillis(), commands);
        AppendTask task = new AppendTask(record, force, false);
        try {
            if (!queue.offer(task, 5, TimeUnit.SECONDS)) throw new IOException("AOF queue is full");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while appending AOF record", e);
        }
        return task.completion;
    }

    private CompletableFuture<Void> enqueueForce() throws IOException {
        IOException currentFailure = failure;
        if (currentFailure != null) throw new IOException("AOF writer is unavailable", currentFailure);
        if (!started.get()) throw new IOException("AOF writer has not been started");
        AppendTask task = new AppendTask(null, true, false);
        try {
            if (!queue.offer(task, 5, TimeUnit.SECONDS)) throw new IOException("AOF queue is full");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while forcing AOF", e);
        }
        return task.completion;
    }

    private static void await(CompletableFuture<Void> completion) throws IOException {
        try {
            completion.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for AOF persistence", e);
        } catch (Exception e) {
            throw new IOException("Failed while waiting for AOF persistence", e);
        }
    }

    private void writerLoop() {
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
        boolean dirty = false;
        long lastForceNanos = System.nanoTime();
        List<AppendTask> inFlight = List.of();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            while (true) {
                AppendTask first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    if (dirty && flushPolicy == AofFlushPolicy.EVERYSEC
                            && System.nanoTime() - lastForceNanos >= TimeUnit.SECONDS.toNanos(1)) {
                        channel.force(false);
                        dirty = false;
                        lastForceNanos = System.nanoTime();
                    }
                    continue;
                }
                if (first.poison) break;

                List<AppendTask> batch = new ArrayList<>(MAX_BATCH_SIZE);
                batch.add(first);
                queue.drainTo(batch, MAX_BATCH_SIZE - 1);
                inFlight = batch;
                boolean forceBatch = flushPolicy == AofFlushPolicy.ALWAYS;
                boolean wroteRecord = false;
                for (AppendTask task : batch) {
                    if (task.poison || task.record == null) {
                        forceBatch |= task.force;
                        continue;
                    }
                    writeRecord(channel, writeBuffer, task.record);
                    wroteRecord = true;
                    forceBatch |= task.force;
                }
                flush(channel, writeBuffer);
                dirty |= wroteRecord;
                boolean everySecondDue = flushPolicy == AofFlushPolicy.EVERYSEC
                        && System.nanoTime() - lastForceNanos >= TimeUnit.SECONDS.toNanos(1);
                if (forceBatch || everySecondDue) {
                    channel.force(false);
                    dirty = false;
                    lastForceNanos = System.nanoTime();
                }
                for (AppendTask task : batch) if (!task.poison) task.completion.complete(null);
                inFlight = List.of();
                if (batch.stream().anyMatch(task -> task.poison)) break;
            }
            flush(channel, writeBuffer);
            if (dirty || flushPolicy != AofFlushPolicy.NO) channel.force(false);
        } catch (Exception e) {
            IOException io = e instanceof IOException value ? value : new IOException("AOF writer failed", e);
            failure = io;
            logger.error("AOF writer stopped unexpectedly", e);
            for (AppendTask task : inFlight) task.completion.completeExceptionally(io);
            AppendTask task;
            while ((task = queue.poll()) != null) task.completion.completeExceptionally(io);
        }
    }

    private static void writeRecord(FileChannel channel, ByteBuffer buffer, byte[] record) throws IOException {
        if (record.length > buffer.capacity()) {
            flush(channel, buffer);
            ByteBuffer large = ByteBuffer.wrap(record);
            while (large.hasRemaining()) channel.write(large);
            return;
        }
        if (buffer.remaining() < record.length) flush(channel, buffer);
        buffer.put(record);
    }

    private static void flush(FileChannel channel, ByteBuffer buffer) throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) channel.write(buffer);
        buffer.clear();
    }

    private static int readFully(FileChannel channel, ByteBuffer target, long position) throws IOException {
        int total = 0;
        while (target.hasRemaining()) {
            int read = channel.read(target, position + total);
            if (read < 0) break;
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    private void truncateTail(FileChannel channel, long position, String reason) throws IOException {
        logger.warn("Truncating invalid AOF tail at byte {}: {}", position, reason);
        channel.truncate(position);
        channel.force(true);
    }

    private static List<AofCommand> canonicalize(String name, String[] args, CommandResponse response, long now) {
        String command = name.toUpperCase();
        if ("SETEX".equals(command) && args.length >= 3) {
            long expireAt = saturatedAdd(now, saturatedMultiply(Long.parseLong(args[1]), 1000));
            return List.of(new AofCommand("SET", new String[]{args[0], args[2]}),
                    new AofCommand("PEXPIREAT", new String[]{args[0], Long.toString(expireAt)}));
        }
        if ("EXPIRE".equals(command) && args.length >= 2) {
            long expireAt = saturatedAdd(now, saturatedMultiply(Long.parseLong(args[1]), 1000));
            return List.of(new AofCommand("PEXPIREAT", new String[]{args[0], Long.toString(expireAt)}));
        }
        if ("SPOP".equals(command) && response instanceof CommandResponse.BulkString bulk
                && bulk.value() != null) {
            return List.of(new AofCommand("SREM", new String[]{args[0], bulk.value()}));
        }
        if ("SPOP".equals(command)) return List.of();
        return List.of(new AofCommand(command, Arrays.copyOf(args, args.length)));
    }

    private static List<AofCommand> snapshotCommands(SnapshotEntry entry) {
        List<AofCommand> commands = new ArrayList<>();
        switch (entry.type()) {
            case STRING -> commands.add(new AofCommand("SET", new String[]{entry.key(), (String) entry.value()}));
            case LIST -> {
                List<String> values = cast(entry.value());
                if (!values.isEmpty()) commands.add(new AofCommand("RPUSH", prepend(entry.key(), values)));
            }
            case SET -> {
                Set<String> values = cast(entry.value());
                if (!values.isEmpty()) commands.add(new AofCommand("SADD", prepend(entry.key(), values)));
            }
            case HASH -> {
                Map<String, String> values = cast(entry.value());
                if (!values.isEmpty()) {
                    List<String> args = new ArrayList<>();
                    args.add(entry.key());
                    values.forEach((field, value) -> { args.add(field); args.add(value); });
                    commands.add(new AofCommand("HSET", args.toArray(new String[0])));
                }
            }
            default -> { }
        }
        if (entry.expireAt() > 0) commands.add(new AofCommand("PEXPIREAT",
                new String[]{entry.key(), Long.toString(entry.expireAt())}));
        return commands;
    }

    private static String[] prepend(String key, Iterable<String> values) {
        List<String> args = new ArrayList<>();
        args.add(key);
        values.forEach(args::add);
        return args.toArray(new String[0]);
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) { return (T) value; }

    private static long saturatedMultiply(long value, long multiplier) {
        try { return Math.multiplyExact(value, multiplier); }
        catch (ArithmeticException e) { return value < 0 ? Long.MIN_VALUE : Long.MAX_VALUE; }
    }

    private static long saturatedAdd(long left, long right) {
        try { return Math.addExact(left, right); }
        catch (ArithmeticException e) { return right < 0 ? Long.MIN_VALUE : Long.MAX_VALUE; }
    }

    @Override
    public void close() {
        if (!started.compareAndSet(true, false)) return;
        AppendTask poison = new AppendTask(null, true, true);
        try {
            queue.put(poison);
            writerThread.join(30_000);
            if (writerThread.isAlive()) {
                writerThread.interrupt();
                logger.warn("Timed out waiting for AOF writer to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Path getPath() { return path; }
    public AofFlushPolicy getFlushPolicy() { return flushPolicy; }

    private static final class AppendTask {
        private final byte[] record;
        private final boolean force;
        private final boolean poison;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private AppendTask(byte[] record, boolean force, boolean poison) {
            this.record = record;
            this.force = force;
            this.poison = poison;
        }
    }
}
