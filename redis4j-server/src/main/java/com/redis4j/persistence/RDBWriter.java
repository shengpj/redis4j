package com.redis4j.persistence;

import com.redis4j.storage.DataType;
import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import com.redis4j.storage.snapshot.SnapshotProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis4J 快照文件写入器。
 *
 * <p>写入时使用稳定的 {@link DataSnapshot}，避免保存过程中业务线程继续修改数据，
 * 导致同一份快照前后内容不一致。</p>
 */
public class RDBWriter {
    private static final Logger logger = LoggerFactory.getLogger(RDBWriter.class);
    private static final byte[] RDB_HEADER = "REDIS0011".getBytes(StandardCharsets.US_ASCII);
    private static final int BUFFER_SIZE = 64 * 1024;

    private FileChannel channel;
    private ByteBuffer buffer;
    // 按数据类型注册序列化策略，新增类型时无需在主写入流程中增加大量分支。
    private final Map<DataType, SnapshotValueWriter> codecs = createCodecs();

    public synchronized void save(SnapshotProvider snapshotProvider, String filePath) throws IOException {
        logger.info("Saving RDB file to {}", filePath);
        Path target = Path.of(filePath).toAbsolutePath();
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);
        // 始终先写同目录临时文件，防止写入中断时截断或破坏上一份可用快照。
        Path temporary = Files.createTempFile(parent, target.getFileName().toString() + ".", ".tmp");

        long start = System.currentTimeMillis();
        // 在开始文件 I/O 前生成稳定快照，后续写入只读取该快照。
        DataSnapshot snapshot = snapshotProvider.createSnapshot();
        boolean moved = false;
        try {
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                channel = output;
                // 复用直接内存缓冲区聚合小字段，减少 FileChannel.write 调用和堆内存到本地内存的复制。
                buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                writeHeader();
                writeEntries(snapshot);
                writeFooter();
                flushBuffer();
                // 确保文件内容进入持久化设备后，才替换正式快照。
                channel.force(true);
            }
            try {
                // 临时文件与目标文件位于同一目录，优先通过原子移动一次性替换旧快照。
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // 某些文件系统不支持原子移动，此时退化为普通覆盖移动。
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
            moved = true;
        } finally {
            channel = null;
            buffer = null;
            if (!moved) Files.deleteIfExists(temporary);
        }
        logger.info("RDB file saved successfully in {} ms: {}", System.currentTimeMillis() - start, filePath);
    }

    private void writeEntries(DataSnapshot snapshot) throws IOException {
        int count = 0;
        for (SnapshotEntry entry : snapshot.entries()) {
            SnapshotValueWriter codec = codecs.get(entry.type());
            // 保存期间已经过期的键没有恢复价值，直接从快照文件中跳过。
            if (codec == null || entry.expireAt() > 0 && entry.expireAt() <= System.currentTimeMillis()) continue;
            // 单条记录格式：记录标识 + key + 过期信息 + 类型 + 类型对应的数据。
            writeByte(0xC0);
            writeString(entry.key());
            boolean expires = entry.expireAt() > 0;
            writeByte(expires ? 1 : 0);
            if (expires) writeInt64(entry.expireAt());
            codec.write(entry);
            count++;
        }
        logger.debug("Wrote {} key-value pairs to RDB", count);
    }

    private Map<DataType, SnapshotValueWriter> createCodecs() {
        Map<DataType, SnapshotValueWriter> result = new EnumMap<>(DataType.class);
        result.put(DataType.STRING, entry -> {
            writeByte(0x00);
            writeString((String) entry.value());
        });
        result.put(DataType.LIST, entry -> {
            writeByte(0x01);
            List<String> values = cast(entry.value());
            writeInt32(values.size());
            for (String value : values) writeString(value);
        });
        result.put(DataType.SET, entry -> {
            writeByte(0x02);
            Set<String> values = cast(entry.value());
            writeInt32(values.size());
            for (String value : values) writeString(value);
        });
        result.put(DataType.HASH, entry -> {
            writeByte(0x04);
            Map<String, String> values = cast(entry.value());
            writeInt32(values.size());
            for (Map.Entry<String, String> value : values.entrySet()) {
                writeString(value.getKey());
                writeString(value.getValue());
            }
        });
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) { return (T) value; }

    private void writeHeader() throws IOException {
        // 文件头包含魔数、格式版本和数据库编号，Reader 会严格校验这些字段。
        ensure(18);
        buffer.put(RDB_HEADER);
        buffer.putInt(9);
        writeByte(0xFE);
        buffer.putInt(0);
    }

    private void writeFooter() throws IOException {
        // 0xFF 表示快照结束；当前格式预留 4 字节校验和字段，值固定为 0。
        ensure(5);
        writeByte(0xFF);
        buffer.putInt(0);
    }

    private void writeString(String value) throws IOException {
        if (value == null) {
            writeInt32(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeInt32(bytes.length);
        if (bytes.length > buffer.capacity()) {
            // 大字段不复制进聚合缓冲区：先刷出小字段，再直接循环写入 Channel。
            flushBuffer();
            ByteBuffer largeValue = ByteBuffer.wrap(bytes);
            // FileChannel 单次 write 不保证写完，必须循环到缓冲区无剩余数据。
            while (largeValue.hasRemaining()) channel.write(largeValue);
        } else {
            ensure(bytes.length);
            buffer.put(bytes);
        }
    }

    private void writeInt32(int value) throws IOException { ensure(4); buffer.putInt(value); }
    private void writeInt64(long value) throws IOException { ensure(8); buffer.putLong(value); }
    private void writeByte(int value) throws IOException { ensure(1); buffer.put((byte) value); }

    private void ensure(int bytes) throws IOException {
        if (bytes > buffer.capacity()) throw new IOException("RDB value exceeds write buffer");
        // 当前空间不足时先刷盘，保证随后的 put 不会越过 ByteBuffer 边界。
        if (buffer.remaining() < bytes) flushBuffer();
    }

    private void flushBuffer() throws IOException {
        // 写模式切换为读模式，完整写入后再 clear，恢复为下一轮写模式。
        buffer.flip();
        while (buffer.hasRemaining()) channel.write(buffer);
        buffer.clear();
    }

    @FunctionalInterface
    private interface SnapshotValueWriter {
        void write(SnapshotEntry entry) throws IOException;
    }
}
