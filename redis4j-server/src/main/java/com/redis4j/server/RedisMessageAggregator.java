package com.redis4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.redis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis 消息聚合器
 * 将 RedisDecoder 分片输出的消息组合成完整的消息
 */
public class RedisMessageAggregator extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(RedisMessageAggregator.class);

    // 聚合状态
    private int expectedArrayLength = -1;
    private List<RedisMessage> arrayElements = null;

    // 待配对的 bulk string header 栈
    private final List<BulkStringHeaderRedisMessage> pendingHeaders = new ArrayList<>();
    // 待配对的 bulk string content 栈
    private final List<byte[]> pendingContents = new ArrayList<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        RedisMessage message = (RedisMessage) msg;

        // 处理 bulk string 内容 - 直接读取字节并保存
        if (msg instanceof DefaultLastBulkStringRedisContent) {
            ByteBuf content = ((DefaultLastBulkStringRedisContent) msg).content();
            if (content != null && content.isReadable()) {
                byte[] bytes = new byte[content.readableBytes()];
                content.getBytes(content.readerIndex(), bytes);
                pendingContents.add(bytes);
            } else {
                pendingContents.add(null);
            }
            checkArrayComplete(ctx);
            return;
        }

        if (msg instanceof DefaultBulkStringRedisContent) {
            ByteBuf content = ((DefaultBulkStringRedisContent) msg).content();
            if (content != null && content.isReadable()) {
                byte[] bytes = new byte[content.readableBytes()];
                content.getBytes(content.readerIndex(), bytes);
                
                // 合并到最后一个 pending content
                if (!pendingContents.isEmpty() && pendingContents.get(pendingContents.size() - 1) != null) {
                    byte[] last = pendingContents.remove(pendingContents.size() - 1);
                    byte[] combined = new byte[last.length + bytes.length];
                    System.arraycopy(last, 0, combined, 0, last.length);
                    System.arraycopy(bytes, 0, combined, last.length, bytes.length);
                    pendingContents.add(combined);
                } else {
                    pendingContents.add(bytes);
                }
            } else {
                pendingContents.add(null);
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
                byte[] bytes = this.pendingContents.remove(pendingContents.size() - 1);
                if (bytes != null && bytes.length > 0) {
                    // 创建新的 ByteBuf 并持有它
                    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                    this.arrayElements.add(new FullBulkStringRedisMessage(buf));
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
