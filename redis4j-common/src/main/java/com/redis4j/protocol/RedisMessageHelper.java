package com.redis4j.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RedisMessage 辅助类，用于构建 RESP 响应
 * 替代之前的 RespObject
 */
public final class RedisMessageHelper {

    private RedisMessageHelper() {
    }

    /**
     * 创建简单字符串响应: +value\r\n
     */
    public static RedisMessage simpleString(String value) {
        return new SimpleStringRedisMessage(value != null ? value : "");
    }

    /**
     * 创建错误响应: -ERR message\r\n
     */
    public static RedisMessage error(String message) {
        return new ErrorRedisMessage(message != null ? message : "");
    }

    /**
     * 创建错误响应: -ERR message\r\n
     */
    public static RedisMessage error(String prefix, String message) {
        return new ErrorRedisMessage(prefix + " " + message);
    }

    /**
     * 创建整数响应: :value\r\n
     */
    public static RedisMessage integer(long value) {
        return new IntegerRedisMessage(value);
    }

    /**
     * 创建 bulk string 响应: $length\r\nvalue\r\n
     */
    public static RedisMessage bulkString(String value) {
        if (value == null) {
            return nullBulkString();
        }
        ByteBuf buf = Unpooled.copiedBuffer(value, StandardCharsets.UTF_8);
        // 使用 unreleasableBuffer 防止 buffer 被意外释放
        return new FullBulkStringRedisMessage(Unpooled.unreleasableBuffer(buf));
    }

    /**
     * 创建 bulk string 响应: $length\r\nvalue\r\n
     */
    public static RedisMessage bulkString(byte[] value) {
        if (value == null) {
            return nullBulkString();
        }
        ByteBuf buf = Unpooled.copiedBuffer(value);
        // 使用 unreleasableBuffer 防止 buffer 被意外释放
        return new FullBulkStringRedisMessage(Unpooled.unreleasableBuffer(buf));
    }

    /**
     * 创建 null bulk string: $-1\r\n
     */
    public static RedisMessage nullBulkString() {
        return FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    /**
     * 创建数组响应: *count\r\n...
     */
    public static RedisMessage array(List<RedisMessage> values) {
        if (values == null) {
            return nullArray();
        }
        return new ArrayRedisMessage(values);
    }

    /**
     * 创建数组响应（可变参数）: *n\r\n...
     */
    public static RedisMessage array(RedisMessage... values) {
        if (values == null) {
            return nullArray();
        }
        List<RedisMessage> list = new ArrayList<>(values.length);
        for (RedisMessage msg : values) {
            list.add(msg != null ? msg : nullBulkString());
        }
        return new ArrayRedisMessage(list);
    }

    /**
     * 创建 null array: *-1\r\n
     */
    public static RedisMessage nullArray() {
        return ArrayRedisMessage.NULL_INSTANCE;
    }

    /**
     * OK 响应
     */
    public static RedisMessage ok() {
        return simpleString("OK");
    }

    /**
     * PONG 响应
     */
    public static RedisMessage pong() {
        return simpleString("PONG");
    }

    /**
     * 从 RedisMessage 提取字符串（用于 bulk string）
     */
    public static String extractString(RedisMessage msg) {
        if (msg == null) {
            return null;
        }
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) {
                return null;
            }
            ByteBuf buf = bulk.content();
            if (buf == null || !buf.isReadable()) {
                return "";
            }
            return buf.toString(StandardCharsets.UTF_8);
        }
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        }
        if (msg instanceof ErrorRedisMessage) {
            return ((ErrorRedisMessage) msg).content();
        }
        return msg.toString();
    }

    /**
     * 从 RedisMessage 提取长整数
     */
    public static long extractLong(RedisMessage msg) {
        if (msg instanceof IntegerRedisMessage) {
            return ((IntegerRedisMessage) msg).value();
        }
        String str = extractString(msg);
        if (str == null) {
            return 0;
        }
        return Long.parseLong(str);
    }

    /**
     * 从 RedisMessage 提取数组
     */
    @SuppressWarnings("unchecked")
    public static List<RedisMessage> extractArray(RedisMessage msg) {
        if (msg instanceof ArrayRedisMessage) {
            ArrayRedisMessage array = (ArrayRedisMessage) msg;
            if (array.isNull()) {
                return null;
            }
            return new ArrayList<>(array.children());
        }
        return new ArrayList<>();
    }

    /**
     * 检查是否为 null bulk string
     */
    public static boolean isNullBulkString(RedisMessage msg) {
        return msg instanceof FullBulkStringRedisMessage
                && ((FullBulkStringRedisMessage) msg).isNull();
    }

    /**
     * 检查是否为 null array
     */
    public static boolean isNullArray(RedisMessage msg) {
        return msg instanceof ArrayRedisMessage
                && ((ArrayRedisMessage) msg).isNull();
    }
}
