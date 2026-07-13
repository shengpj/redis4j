package com.redis4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 按 RESP 编码后的实际字节数限制每个客户端尚未完成的出站数据。 */
final class ClientOutputBufferLimitHandler extends ChannelOutboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ClientOutputBufferLimitHandler.class);
    private static final AttributeKey<Long> PENDING_BYTES =
            AttributeKey.valueOf("redis4j.pendingOutputBytes");

    private final PubSubBroker pubSubBroker;
    private final long normalLimit;
    private final long pubSubLimit;
    private long pendingBytes;

    ClientOutputBufferLimitHandler(PubSubBroker pubSubBroker, long normalLimit, long pubSubLimit) {
        if (normalLimit < 0 || pubSubLimit < 0) {
            throw new IllegalArgumentException("Client output buffer limits cannot be negative");
        }
        this.pubSubBroker = pubSubBroker;
        this.normalLimit = normalLimit;
        this.pubSubLimit = pubSubLimit;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
        int messageBytes = readableBytes(message);
        long limit = pubSubBroker.isSubscribed(ctx.channel()) ? pubSubLimit : normalLimit;
        if (limit > 0 && messageBytes > limit - pendingBytes) {
            TooLongFrameException failure = new TooLongFrameException(
                    "Client output buffer limit exceeded: pending=" + pendingBytes
                            + ", message=" + messageBytes + ", limit=" + limit);
            logger.warn("Client output buffer limit exceeded, closing: {}", ctx.channel().remoteAddress());
            ReferenceCountUtil.release(message);
            promise.tryFailure(failure);
            ctx.close();
            return;
        }

        pendingBytes += messageBytes;
        updatePendingBytes(ctx.channel());
        promise.addListener(ignored -> {
            pendingBytes = Math.max(0, pendingBytes - messageBytes);
            updatePendingBytes(ctx.channel());
        });
        ctx.write(message, promise);
    }

    static long pendingBytes(Channel channel) {
        Long value = channel.attr(PENDING_BYTES).get();
        return value == null ? 0 : value;
    }

    private void updatePendingBytes(Channel channel) {
        channel.attr(PENDING_BYTES).set(pendingBytes);
    }

    private static int readableBytes(Object message) {
        if (message instanceof ByteBuf buffer) return buffer.readableBytes();
        if (message instanceof ByteBufHolder holder) return holder.content().readableBytes();
        return 0;
    }
}
