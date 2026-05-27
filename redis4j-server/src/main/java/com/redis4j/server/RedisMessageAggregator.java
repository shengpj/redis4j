package com.redis4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.redis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis 消息聚合器
 * 将 RedisDecoder 分片输出的消息组合成完整的消息
 *
 * 状态机：
 * - IDLE: 等待接收数组头
 * - COLLECTING_ARRAY: 正在收集数组元素
 */
public class RedisMessageAggregator extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(RedisMessageAggregator.class);

    // 状态枚举
    private enum State {
        IDLE,
        COLLECTING_ARRAY
    }

    private State state = State.IDLE;
    private int expectedArrayLength = -1;
    private final List<RedisMessage> arrayElements = new ArrayList<>();

    // 队列：待配对的 bulk string header 和 content
    // 使用 FIFO 队列确保顺序正确
    private final List<BulkStringHeaderRedisMessage> headerQueue = new ArrayList<>();
    private final List<byte[]> contentQueue = new ArrayList<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        try {
            processMessage(ctx, (RedisMessage) msg);
        } catch (Exception e) {
            logger.error("Error processing message", e);
            ctx.fireChannelRead(msg);
        }
    }

    private void processMessage(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        switch (state) {
            case IDLE:
                processIdleState(ctx, msg);
                break;
            case COLLECTING_ARRAY:
                processCollectingState(ctx, msg);
                break;
        }
    }

    /**
     * IDLE 状态：等待数组头
     */
    private void processIdleState(ChannelHandlerContext ctx, RedisMessage msg) {
        if (msg instanceof ArrayHeaderRedisMessage) {
            ArrayHeaderRedisMessage header = (ArrayHeaderRedisMessage) msg;
            if (header.isNull()) {
                // *-1\r\n 表示 null 数组
                ctx.fireChannelRead(ArrayRedisMessage.NULL_INSTANCE);
                return;
            }

            expectedArrayLength = (int) header.length();
            if (expectedArrayLength == 0) {
                // *0\r\n 表示空数组
                ctx.fireChannelRead(new ArrayRedisMessage(new ArrayList<>()));
                return;
            }

            // 开始收集数组元素
            arrayElements.clear();
            headerQueue.clear();
            contentQueue.clear();
            state = State.COLLECTING_ARRAY;

        } else if (msg instanceof BulkStringHeaderRedisMessage) {
            // 单个 bulk string 命令（如 PING）
            headerQueue.add((BulkStringHeaderRedisMessage) msg);
            state = State.COLLECTING_ARRAY;
            expectedArrayLength = 1;
            arrayElements.clear();

        } else {
            // 其他消息类型直接传递
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * COLLECTING_ARRAY 状态：正在收集数组元素
     */
    private void processCollectingState(ChannelHandlerContext ctx, RedisMessage msg) {
        if (msg instanceof BulkStringHeaderRedisMessage) {
            headerQueue.add((BulkStringHeaderRedisMessage) msg);

        } else if (msg instanceof DefaultLastBulkStringRedisContent) {
            ByteBuf content = ((DefaultLastBulkStringRedisContent) msg).content();
            contentQueue.add(readBytes(content));

        } else if (msg instanceof DefaultBulkStringRedisContent) {
            ByteBuf content = ((DefaultBulkStringRedisContent) msg).content();
            contentQueue.add(readBytes(content));

        } else if (msg instanceof ArrayHeaderRedisMessage) {
            // 嵌套数组 - 先输出当前数组，再处理新数组
            // （实际上 Redis 协议中数组不会这样嵌套，这里是防御性处理）
            if (!headerQueue.isEmpty() || !contentQueue.isEmpty()) {
                emitCurrentArray(ctx);
            }
            processIdleState(ctx, msg);

        } else {
            // 其他消息类型（如 SimpleString, Integer）直接添加
            arrayElements.add(msg);
        }

        // 尝试配对并检查是否完成
        matchAndCheckComplete(ctx);
    }

    /**
     * 尝试配对 header 和 content
     */
    private void matchAndCheckComplete(ChannelHandlerContext ctx) {
        while (!headerQueue.isEmpty()) {
            BulkStringHeaderRedisMessage header = headerQueue.remove(0);

            if (header.isNull()) {
                arrayElements.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                continue;
            }

            if (contentQueue.isEmpty()) {
                // 没有 content，等待
                headerQueue.add(0, header);
                break;
            }

            byte[] bytes = contentQueue.remove(0);
            if (bytes != null && bytes.length > 0) {
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                arrayElements.add(new FullBulkStringRedisMessage(buf));
            } else {
                arrayElements.add(FullBulkStringRedisMessage.NULL_INSTANCE);
            }
        }

        // 检查是否完成
        if (arrayElements.size() == expectedArrayLength) {
            emitCurrentArray(ctx);
        }
    }

    /**
     * 输出当前数组并重置状态
     */
    private void emitCurrentArray(ChannelHandlerContext ctx) {
        ctx.fireChannelRead(new ArrayRedisMessage(new ArrayList<>(arrayElements)));
        reset();
    }

    /**
     * 重置状态
     */
    private void reset() {
        state = State.IDLE;
        expectedArrayLength = -1;
        arrayElements.clear();
        headerQueue.clear();
        contentQueue.clear();
    }

    /**
     * 从 ByteBuf 读取字节数组
     */
    private byte[] readBytes(ByteBuf buf) {
        if (buf == null || !buf.isReadable()) {
            return null;
        }
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return bytes;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error in RedisMessageAggregator", cause);
        reset();
        ctx.close();
    }
}
