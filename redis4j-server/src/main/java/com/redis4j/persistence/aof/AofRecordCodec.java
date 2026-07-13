package com.redis4j.persistence.aof;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/** RocketMQ CommitLog 风格的定长头 + 变长负载记录格式。 */
final class AofRecordCodec {
    static final int MAGIC = 0x524A414F; // "RJAO"
    static final byte VERSION = 1;
    static final int HEADER_SIZE = Integer.BYTES + 1 + Long.BYTES + Integer.BYTES + Integer.BYTES;
    static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

    private AofRecordCodec() {}

    static byte[] encode(long timestamp, List<AofCommand> commands) throws IOException {
        byte[] payload = encodePayload(commands);
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        ByteBuffer record = ByteBuffer.allocate(HEADER_SIZE + payload.length);
        record.putInt(MAGIC);
        record.put(VERSION);
        record.putLong(timestamp);
        record.putInt(payload.length);
        record.putInt((int) crc32.getValue());
        record.put(payload);
        return record.array();
    }

    static List<AofCommand> decode(byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            int commandCount = input.readInt();
            if (commandCount <= 0 || commandCount > 1_000_000) {
                throw new IOException("Invalid AOF command count: " + commandCount);
            }
            List<AofCommand> commands = new ArrayList<>(commandCount);
            for (int i = 0; i < commandCount; i++) {
                String name = readString(input);
                int argumentCount = input.readInt();
                if (argumentCount < 0 || argumentCount > 1_000_000) {
                    throw new IOException("Invalid AOF argument count: " + argumentCount);
                }
                String[] args = new String[argumentCount];
                for (int j = 0; j < argumentCount; j++) args[j] = readString(input);
                commands.add(new AofCommand(name, args));
            }
            if (input.available() != 0) throw new IOException("Trailing bytes in AOF payload");
            return commands;
        }
    }

    static boolean checksumMatches(byte[] payload, int expected) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return (int) crc32.getValue() == expected;
    }

    private static byte[] encodePayload(List<AofCommand> commands) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(commands.size());
            for (AofCommand command : commands) {
                writeString(output, command.name());
                String[] args = command.args();
                output.writeInt(args.length);
                for (String arg : args) writeString(output, arg);
            }
        }
        byte[] payload = bytes.toByteArray();
        if (payload.length > MAX_PAYLOAD_SIZE) throw new IOException("AOF record exceeds maximum payload size");
        return payload;
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > MAX_PAYLOAD_SIZE || length > input.available()) {
            throw new IOException("Invalid AOF string length: " + length);
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
