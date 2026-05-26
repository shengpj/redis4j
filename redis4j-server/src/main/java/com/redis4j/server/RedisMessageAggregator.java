package com.redis4j.server;

import io.netty.buffer.ByteBuf;
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
 * 关键：Redis 协议中 bulk string 的 header 和 content 可能不按顺序到达。
 * 例如：BulkStringHeader -> BulkStringHeader -> Content -> Content
 * 我们需要正确地将 header 和 content 配对。
 */
public class RedisMessageAggregator extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(RedisMessageAggregator.class);

    // 聚合状态
    private int expectedArrayLength = -1;
    private List<RedisMessage> arrayElements = null;

    // 待配对的 bulk string header 栈（LIFO，用于配对）
    private final List<BulkStringHeaderRedisMessage> pendingHeaders = new ArrayList<>();
    // 待配对的 bulk string content 栈（LIFO，用于配对）
    private final List<ByteBuf> pendingContents = new ArrayList<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        RedisMessage message = (RedisMessage) msg;

        // 处理 bulk string 内容
        if (msg instanceof DefaultLastBulkStringRedisContent) {
            ByteBuf content = ((DefaultLastBulkStringRedisContent) msg).content();
            if (content != null && content.isReadable()) {
                pendingContents.add(content.copy());
            }
            checkArrayComplete(ctx);
            return;
        }

        if (msg instanceof DefaultBulkStringRedisContent) {
            ByteBuf content = ((DefaultBulkStringRedisContent) msg).content();
            if (content != null && content.isReadable()) {
                // 合并到最后一个 pending content
                ByteBuf last = pendingContents.isEmpty() ? null : pendingContents.get(pendingContents.size() - 1);
                if (last == null) {
                    pendingContents.add(content.copy());
                } else {
                    byte[] combined = new byte[last.readableBytes() + content.readableBytes()];
                    last.getBytes(last.readerIndex(), combined, 0, last.readableBytes());
                    content.getBytes(content.readerIndex(), combined, last.readableBytes(), content.readableBytes());
                    pendingContents.set(pendingContents.size() - 1, ctx.alloc().buffer(combined.length).writeBytes(combined));
                }
            }
            checkArrayComplete(ctx);
            return;
        }

        // 处理 bulk string header
        if (msg instanceof BulkStringHeaderRedisMessage) {
            pendingHeaders.add((BulkStringHeaderRedisMessage) msg);
            checkArrayComplete(ctx);
            return;
        }

        // 处理数组 header
        if (msg instanceof ArrayHeaderRedisMessage) {
            ArrayHeaderRedisMessage header = (ArrayHeaderRedisMessage) msg;
            if (header.isNull()) {
                ctx.fireChannelRead(ArrayRedisMessage.NULL_INSTANCE);
            } else {
                // 重置状态
                this.expectedArrayLength = (int) header.length();
                this.arrayElements = new ArrayList<>(expectedArrayLength);
                this.pendingHeaders.clear();
                this.pendingContents.clear();

                if (expectedArrayLength == 0) {
                    ctx.fireChannelRead(new ArrayRedisMessage(new ArrayList<>()));
                    this.arrayElements = null;
                }
            }
            return;
        }

        // 处理数组元素（已经是完整类型）
        if (this.arrayElements != null) {
            this.arrayElements.add(message);
            checkArrayComplete(ctx);
            return;
        }

        // 其他消息类型直接传递
        ctx.fireChannelRead(message);
    }

    /**
     * 检查数组是否完成，并配对 header + content
     */
    private void checkArrayComplete(ChannelHandlerContext ctx) {
        if (this.arrayElements == null) {
            return;
        }

        // 尝试配对 header + content
        while (!this.pendingHeaders.isEmpty()) {
            BulkStringHeaderRedisMessage header = this.pendingHeaders.remove(pendingHeaders.size() - 1);

            if (header.isNull()) {
                this.arrayElements.add(FullBulkStringRedisMessage.NULL_INSTANCE);
            } else if (!this.pendingContents.isEmpty()) {
                ByteBuf content = this.pendingContents.remove(pendingContents.size() - 1);
                if (content != null && content.isReadable()) {
                    this.arrayElements.add(new FullBulkStringRedisMessage(content));
                } else {
                    this.arrayElements.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                }
            } else {
                // 没有 content，等待
                this.pendingHeaders.add(header);
                break;
            }
        }

        // 检查数组是否完成
        if (this.arrayElements != null && this.arrayElements.size() == this.expectedArrayLength) {
            List<RedisMessage> elements = new ArrayList<>(this.arrayElements);
            ctx.fireChannelRead(new ArrayRedisMessage(elements));
            this.arrayElements = null;
            this.expectedArrayLength = -1;
            this.pendingHeaders.clear();
            this.pendingContents.clear();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Error in RedisMessageAggregator", cause);
        ctx.close();
    }
}
