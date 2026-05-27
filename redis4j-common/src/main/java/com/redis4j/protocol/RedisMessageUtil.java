package com.redis4j.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RedisMessage 工具类
 */
public final class RedisMessageUtil {

    private RedisMessageUtil() {
    }

    /**
     * 深度复制 RedisMessage，避免 ByteBuf 引用问题
     */
    public static RedisMessage deepCopy(RedisMessage msg) {
        if (msg == null) {
            return null;
        }

        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            ByteBuf content = bulk.content();
            if (content == null || !content.isReadable()) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            byte[] bytes = new byte[content.readableBytes()];
            content.getBytes(content.readerIndex(), bytes);
            ByteBuf copy = Unpooled.wrappedBuffer(bytes);
            return new FullBulkStringRedisMessage(copy);
        }

        if (msg instanceof SimpleStringRedisMessage) {
            return new SimpleStringRedisMessage(((SimpleStringRedisMessage) msg).content());
        }

        if (msg instanceof ErrorRedisMessage) {
            return new ErrorRedisMessage(((ErrorRedisMessage) msg).content());
        }

        if (msg instanceof IntegerRedisMessage) {
            return new IntegerRedisMessage(((IntegerRedisMessage) msg).value());
        }

        if (msg instanceof ArrayRedisMessage) {
            ArrayRedisMessage array = (ArrayRedisMessage) msg;
            if (array.isNull()) {
                return ArrayRedisMessage.NULL_INSTANCE;
            }
            List<RedisMessage> copy = new ArrayList<>();
            for (RedisMessage child : array.children()) {
                copy.add(deepCopy(child));
            }
            return new ArrayRedisMessage(copy);
        }

        return msg;
    }

    /**
     * 从 RedisMessage 提取字符串
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

        if (msg instanceof IntegerRedisMessage) {
            return String.valueOf(((IntegerRedisMessage) msg).value());
        }

        return msg.toString();
    }
}
