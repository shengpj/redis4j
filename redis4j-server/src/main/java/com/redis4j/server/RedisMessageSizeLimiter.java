package com.redis4j.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.ReferenceCountUtil;

/** Rejects bulk strings before Netty aggregates their content into one buffer. */
final class RedisMessageSizeLimiter extends ChannelInboundHandlerAdapter {
    private final int maxBulkStringLength;

    RedisMessageSizeLimiter(int maxBulkStringLength) {
        if (maxBulkStringLength <= 0) throw new IllegalArgumentException("maxBulkStringLength must be positive");
        this.maxBulkStringLength = maxBulkStringLength;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        long length = -1;
        if (message instanceof BulkStringHeaderRedisMessage header) {
            length = header.bulkStringLength();
        } else if (message instanceof FullBulkStringRedisMessage full && !full.isNull()) {
            length = full.content().readableBytes();
        }
        if (length > maxBulkStringLength) {
            ReferenceCountUtil.release(message);
            throw new TooLongFrameException("RESP bulk string length " + length
                    + " exceeds configured maximum " + maxBulkStringLength);
        }
        ctx.fireChannelRead(message);
    }
}
