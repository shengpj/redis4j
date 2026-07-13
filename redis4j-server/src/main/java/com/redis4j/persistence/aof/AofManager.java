package com.redis4j.persistence.aof;

import com.redis4j.command.CommandJournal;
import com.redis4j.command.CommandRegistry;
import com.redis4j.command.WriteCommandSupport;
import com.redis4j.persistence.RDBReader;
import com.redis4j.persistence.RDBWriter;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import com.redis4j.storage.snapshot.SnapshotProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
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
    private static final long MAX_REWRITE_BUFFER_SIZE = 64L * 1024 * 1024;
    private static final byte[] HYBRID_MAGIC = "R4JHYBRD".getBytes(StandardCharsets.US_ASCII);
    private static final int HYBRID_VERSION = 1;
    private static final int HYBRID_HEADER_SIZE = HYBRID_MAGIC.length + Integer.BYTES + Long.BYTES;
    private final Path path;
    private final AofFlushPolicy flushPolicy;
    private final ArrayBlockingQueue<AppendTask> queue;
    private final boolean useRdbPreamble;
    private final Object enqueueLock = new Object();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean rewriting = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile IOException failure;
    private volatile RewriteSession rewriteSession;
    private volatile long lastRewriteSize;
    private Thread writerThread;
    private volatile Thread rewriteThread;

    public AofManager(Path path, AofFlushPolicy flushPolicy, int queueCapacity) {
        this(path, flushPolicy, queueCapacity, true);
    }

    public AofManager(Path path, AofFlushPolicy flushPolicy, int queueCapacity, boolean useRdbPreamble) {
        if (queueCapacity <= 0) throw new IllegalArgumentException("AOF queue capacity must be positive");
        this.path = path.toAbsolutePath();
        this.flushPolicy = flushPolicy;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.useRdbPreamble = useRdbPreamble;
    }

    public boolean exists() {
        return Files.exists(path);
    }

    public void start() throws IOException {
        if (closed.get()) throw new IOException("AOF manager is closed");
        if (!started.compareAndSet(false, true)) return;
        Path parent = path.getParent();
        if (parent != null) Files.createDirectories(parent);
        writerThread = new Thread(this::writerLoop, "aof-writer");
        writerThread.setDaemon(true);
        writerThread.start();
        if (lastRewriteSize == 0 && Files.exists(path)) lastRewriteSize = Files.size(path);
    }

    @Override
    public boolean isWriteCommand(String commandName) {
        return WriteCommandSupport.isWriteCommand(commandName);
    }

    @Override
    public void ensureWritable() throws IOException {
        IOException currentFailure = failure;
        if (currentFailure != null) throw new IOException("AOF writer is unavailable", currentFailure);
        if (closed.get()) throw new IOException("AOF manager is closed");
        if (!started.get()) throw new IOException("AOF writer has not been started");
    }

    @Override
    public CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response) throws IOException {
        List<AofCommand> commands = canonicalize(commandName, args, response, System.currentTimeMillis());
        return commands.isEmpty() ? CompletableFuture.completedFuture(null) : enqueueCommands(commands, false);
    }

    @Override
    public CompletableFuture<Void> appendWithEvictions(String commandName, String[] args,
                                                        CommandResponse response, List<String> evictedKeys)
            throws IOException {
        List<AofCommand> commands = new ArrayList<>(canonicalize(commandName, args, response,
                System.currentTimeMillis()));
        if (!evictedKeys.isEmpty()) commands.add(new AofCommand("DEL", evictedKeys.toArray(new String[0])));
        return commands.isEmpty() ? CompletableFuture.completedFuture(null) : enqueueCommands(commands, false);
    }

    public void appendSnapshot(DataSnapshot snapshot) throws IOException {
        for (SnapshotEntry entry : snapshot.entries()) {
            List<AofCommand> commands = snapshotCommands(entry);
            if (!commands.isEmpty()) await(enqueueCommands(commands, false));
        }
        await(enqueueForce());
        lastRewriteSize = Files.size(path);
    }

    /**
     * 后台生成紧凑 AOF。快照写入阶段允许业务写入，期间产生的新记录会同时进入增量缓冲区；
     * 仅在最终切换文件时短暂阻塞写命令。
     */
    public boolean bgRewrite(CommandRegistry registry, SnapshotProvider snapshotProvider) {
        if (!started.get() || closed.get() || !rewriting.compareAndSet(false, true)) return false;
        Thread thread = new Thread(() -> runRewrite(registry, snapshotProvider), "aof-rewrite");
        thread.setDaemon(true);
        rewriteThread = thread;
        thread.start();
        return true;
    }

    public boolean isRewriting() {
        return rewriting.get();
    }

    public boolean isRewriteCapturing() {
        return rewriteSession != null;
    }

    public boolean shouldAutoRewrite(long minimumSize, int growthPercentage) {
        if (minimumSize < 0 || growthPercentage < 0 || rewriting.get() || !started.get()) return false;
        try {
            long currentSize = Files.exists(path) ? Files.size(path) : 0;
            if (currentSize == 0 || currentSize < minimumSize) return false;
            long baseline = lastRewriteSize;
            if (baseline <= 0) return currentSize >= minimumSize;
            if (currentSize <= baseline) return false;
            long requiredGrowth = saturatedAdd(
                    saturatedMultiply(baseline / 100, growthPercentage),
                    (baseline % 100 * growthPercentage + 99) / 100);
            return currentSize - baseline >= requiredGrowth;
        } catch (IOException e) {
            logger.warn("Unable to inspect AOF size for automatic rewrite", e);
            return false;
        }
    }

    public int recover(CommandRegistry registry) throws IOException {
        return recover(registry, null);
    }

    public int recover(CommandRegistry registry, DataStore dataStore) throws IOException {
        if (!Files.exists(path)) return 0;
        HybridHeader hybridHeader = readHybridHeader();
        long aofStart = 0;
        if (hybridHeader != null) {
            if (dataStore == null) {
                throw new IOException("A DataStore is required to recover an AOF file with an RDB preamble");
            }
            new RDBReader().loadRegion(dataStore, path, HYBRID_HEADER_SIZE, hybridHeader.rdbLength());
            aofStart = HYBRID_HEADER_SIZE + hybridHeader.rdbLength();
        }
        int applied = 0;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long position = aofStart;
            long lastGoodPosition = aofStart;
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
        logger.info("AOF recovery completed: {} incremental commands applied from {}", applied, path);
        lastRewriteSize = Files.size(path);
        return applied;
    }

    private CompletableFuture<Void> enqueueCommands(List<AofCommand> commands, boolean force) throws IOException {
        byte[] record = AofRecordCodec.encode(System.currentTimeMillis(), commands);
        AppendTask task = new AppendTask(record, force, false);
        synchronized (enqueueLock) {
            ensureWritable();
            try {
                if (!queue.offer(task, 5, TimeUnit.SECONDS)) throw new IOException("AOF queue is full");
                RewriteSession session = rewriteSession;
                if (session != null) session.capture(record);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while appending AOF record", e);
            }
        }
        return task.completion;
    }

    private CompletableFuture<Void> enqueueForce() throws IOException {
        AppendTask task = new AppendTask(null, true, false);
        synchronized (enqueueLock) {
            ensureWritable();
            try {
                if (!queue.offer(task, 5, TimeUnit.SECONDS)) throw new IOException("AOF queue is full");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while forcing AOF", e);
            }
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
                long batchStart = channel.size();
                try {
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
                } catch (Exception batchFailure) {
                    rollbackFailedBatch(channel, writeBuffer, batchStart, batchFailure);
                    throw batchFailure;
                }
                for (AppendTask task : batch) if (!task.poison) task.completion.complete(null);
                inFlight = List.of();
                if (batch.stream().anyMatch(task -> task.poison)) break;
            }
            flush(channel, writeBuffer);
            if (dirty || flushPolicy != AofFlushPolicy.NO) channel.force(false);
        } catch (Exception e) {
            IOException io = e instanceof IOException value ? value : new IOException("AOF writer failed", e);
            logger.error("AOF writer stopped unexpectedly", e);
            for (AppendTask task : inFlight) task.completion.completeExceptionally(io);
            synchronized (enqueueLock) {
                failure = io;
                AppendTask task;
                while ((task = queue.poll()) != null) task.completion.completeExceptionally(io);
            }
        }
    }

    private static void rollbackFailedBatch(FileChannel channel, ByteBuffer buffer, long batchStart,
                                            Exception originalFailure) {
        buffer.clear();
        try {
            channel.truncate(batchStart);
            channel.force(true);
        } catch (IOException rollbackFailure) {
            originalFailure.addSuppressed(rollbackFailure);
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

    private void runRewrite(CommandRegistry registry, SnapshotProvider snapshotProvider) {
        Path temporary = null;
        try {
            DataSnapshot snapshot = registry.withWriteCommandLock(() -> {
                DataSnapshot stableSnapshot = snapshotProvider.createSnapshot();
                rewriteSession = new RewriteSession(MAX_REWRITE_BUFFER_SIZE);
                return stableSnapshot;
            });
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            temporary = Files.createTempFile(parent, path.getFileName().toString() + ".rewrite.", ".tmp");
            writeSnapshotFile(temporary, snapshot);
            Path completedSnapshot = temporary;
            registry.withWriteCommandLock(() -> {
                finalizeRewrite(completedSnapshot);
                return null;
            });
            temporary = null;
            logger.info("AOF background rewrite completed: {}", path);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("AOF background rewrite interrupted");
        } catch (Exception e) {
            logger.error("AOF background rewrite failed", e);
        } finally {
            rewriteSession = null;
            if (temporary != null) {
                try { Files.deleteIfExists(temporary); }
                catch (IOException e) { logger.warn("Failed to delete AOF rewrite temporary file {}", temporary, e); }
            }
            rewriting.set(false);
            rewriteThread = null;
        }
    }

    private void writeSnapshotFile(Path temporary, DataSnapshot snapshot) throws IOException, InterruptedException {
        if (useRdbPreamble) {
            if (Thread.currentThread().isInterrupted() || closed.get()) throw new InterruptedException();
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer header = ByteBuffer.allocate(HYBRID_HEADER_SIZE);
                header.put(HYBRID_MAGIC).putInt(HYBRID_VERSION).putLong(0).flip();
                while (header.hasRemaining()) channel.write(header);
                long rdbStart = channel.position();
                new RDBWriter().writeSnapshot(snapshot, channel);
                long rdbLength = channel.position() - rdbStart;
                ByteBuffer encodedLength = ByteBuffer.allocate(Long.BYTES);
                encodedLength.putLong(rdbLength).flip();
                writeFully(channel, encodedLength, HYBRID_MAGIC.length + Integer.BYTES);
                channel.force(false);
            }
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (SnapshotEntry entry : snapshot.entries()) {
                if (Thread.currentThread().isInterrupted() || closed.get()) throw new InterruptedException();
                List<AofCommand> commands = snapshotCommands(entry);
                if (!commands.isEmpty()) {
                    writeRecord(channel, buffer, AofRecordCodec.encode(System.currentTimeMillis(), commands));
                }
            }
            flush(channel, buffer);
            channel.force(false);
        }
    }

    private void finalizeRewrite(Path temporary) throws Exception {
        RewriteSession session = rewriteSession;
        if (session == null) throw new IOException("AOF rewrite session is missing");
        if (session.overflowed()) throw new IOException("AOF rewrite buffer exceeded " + MAX_REWRITE_BUFFER_SIZE + " bytes");
        if (closed.get()) throw new InterruptedException("AOF manager is closing");

        // force 任务是旧文件的顺序屏障：完成时，快照之后的所有写命令都已进入旧 AOF。
        await(enqueueForce());
        stopWriter();
        Exception failureDuringSwitch = null;
        boolean switched = false;
        try {
            appendRewriteDelta(temporary, session.records());
            replaceAofFile(temporary);
            switched = true;
            rewriteSession = null;
            failure = null;
        } catch (Exception e) {
            failureDuringSwitch = e;
            throw e;
        } finally {
            if (!closed.get()) {
                try {
                    start();
                    if (switched) lastRewriteSize = Files.size(path);
                } catch (IOException restartFailure) {
                    if (failureDuringSwitch != null) failureDuringSwitch.addSuppressed(restartFailure);
                    else throw restartFailure;
                }
            }
        }
    }

    private void appendRewriteDelta(Path temporary, List<byte[]> records) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            for (byte[] record : records) writeRecord(channel, buffer, record);
            flush(channel, buffer);
            channel.force(false);
        }
    }

    private void replaceAofFile(Path temporary) throws IOException {
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static boolean isHybridFile(Path path) throws IOException {
        if (!Files.exists(path) || Files.size(path) < HYBRID_HEADER_SIZE) return false;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer magic = ByteBuffer.allocate(HYBRID_MAGIC.length);
            return readFully(channel, magic, 0) == HYBRID_MAGIC.length
                    && Arrays.equals(magic.array(), HYBRID_MAGIC);
        }
    }

    private HybridHeader readHybridHeader() throws IOException {
        if (!isHybridFile(path)) return null;
        ByteBuffer header = ByteBuffer.allocate(HYBRID_HEADER_SIZE);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            if (readFully(channel, header, 0) != HYBRID_HEADER_SIZE) {
                throw new IOException("Truncated hybrid AOF header");
            }
        }
        header.flip();
        header.position(HYBRID_MAGIC.length);
        int version = header.getInt();
        long rdbLength = header.getLong();
        long fileSize = Files.size(path);
        if (version != HYBRID_VERSION) throw new IOException("Unsupported hybrid AOF version: " + version);
        if (rdbLength <= 0 || rdbLength > fileSize - HYBRID_HEADER_SIZE) {
            throw new IOException("Invalid hybrid AOF RDB length: " + rdbLength);
        }
        return new HybridHeader(rdbLength);
    }

    private static void writeFully(FileChannel channel, ByteBuffer source, long position) throws IOException {
        long current = position;
        while (source.hasRemaining()) {
            int written = channel.write(source, current);
            if (written <= 0) throw new IOException("Unable to update hybrid AOF header");
            current += written;
        }
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
        if (!closed.compareAndSet(false, true)) return;
        Thread activeRewrite = rewriteThread;
        if (activeRewrite != null && activeRewrite != Thread.currentThread()) {
            activeRewrite.interrupt();
            try { activeRewrite.join(30_000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        try {
            stopWriter();
        } catch (IOException e) {
            logger.error("Failed to stop AOF writer cleanly", e);
        }
    }

    private void stopWriter() throws IOException {
        if (!started.compareAndSet(true, false)) return;
        Thread activeWriter = writerThread;
        if (activeWriter == null || !activeWriter.isAlive()) return;
        AppendTask poison = new AppendTask(null, true, true);
        try {
            if (!queue.offer(poison, 5, TimeUnit.SECONDS)) {
                activeWriter.interrupt();
                throw new IOException("Timed out enqueueing the AOF writer stop signal");
            }
            activeWriter.join(30_000);
            if (activeWriter.isAlive()) {
                activeWriter.interrupt();
                throw new IOException("Timed out waiting for AOF writer to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while stopping AOF writer", e);
        }
    }

    public Path getPath() { return path; }
    public AofFlushPolicy getFlushPolicy() { return flushPolicy; }
    public boolean isUseRdbPreamble() { return useRdbPreamble; }

    private record HybridHeader(long rdbLength) {}

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

    private static final class RewriteSession {
        private final long maximumBytes;
        private final List<byte[]> records = new ArrayList<>();
        private long bytes;
        private boolean overflowed;

        private RewriteSession(long maximumBytes) {
            this.maximumBytes = maximumBytes;
        }

        private synchronized void capture(byte[] record) {
            if (overflowed) return;
            if (bytes + record.length > maximumBytes) {
                overflowed = true;
                records.clear();
                return;
            }
            records.add(record);
            bytes += record.length;
        }

        private synchronized boolean overflowed() {
            return overflowed;
        }

        private synchronized List<byte[]> records() {
            return List.copyOf(records);
        }
    }
}
