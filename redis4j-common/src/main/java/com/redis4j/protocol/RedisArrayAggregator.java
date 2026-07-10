package com.redis4j.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Aggregates RESP arrays after {@code RedisBulkStringAggregator}. */
public class RedisArrayAggregator extends ChannelInboundHandlerAdapter {
    private static final int DEFAULT_MAX_ARRAY_LENGTH = 1_048_576;

    private final int maxArrayLength;
    private final Deque<ArrayFrame> frames = new ArrayDeque<>();

    public RedisArrayAggregator() {
        this(DEFAULT_MAX_ARRAY_LENGTH);
    }

    public RedisArrayAggregator(int maxArrayLength) {
        if (maxArrayLength <= 0) {
            throw new IllegalArgumentException("maxArrayLength must be positive");
        }
        this.maxArrayLength = maxArrayLength;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof RedisMessage redisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }
        if (redisMessage instanceof ArrayHeaderRedisMessage header) {
            try {
                handleHeader(ctx, header);
            } finally {
                ReferenceCountUtil.release(header);
            }
            return;
        }
        if (frames.isEmpty()) {
            ctx.fireChannelRead(redisMessage);
        } else {
            addMessage(ctx, redisMessage);
        }
    }

    private void handleHeader(ChannelHandlerContext ctx, ArrayHeaderRedisMessage header) {
        if (header.isNull()) {
            addMessage(ctx, ArrayRedisMessage.NULL_INSTANCE);
            return;
        }
        long length = header.length();
        if (length < 0 || length > maxArrayLength) {
            throw new IllegalArgumentException("invalid RESP array length: " + length);
        }
        if (length == 0) {
            addMessage(ctx, new ArrayRedisMessage(List.of()));
            return;
        }
        frames.push(new ArrayFrame((int) length));
    }

    private void addMessage(ChannelHandlerContext ctx, RedisMessage message) {
        RedisMessage completed = message;
        while (true) {
            if (frames.isEmpty()) {
                ctx.fireChannelRead(completed);
                return;
            }
            ArrayFrame frame = frames.peek();
            frame.children.add(completed);
            if (frame.children.size() < frame.expectedLength) {
                return;
            }
            frames.pop();
            completed = new ArrayRedisMessage(frame.children);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseFrames();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        releaseFrames();
        super.handlerRemoved(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        releaseFrames();
        ctx.fireExceptionCaught(cause);
    }

    private void releaseFrames() {
        while (!frames.isEmpty()) {
            frames.pop().children.forEach(ReferenceCountUtil::release);
        }
    }

    private static final class ArrayFrame {
        private final int expectedLength;
        private final List<RedisMessage> children;

        private ArrayFrame(int expectedLength) {
            this.expectedLength = expectedLength;
            this.children = new ArrayList<>(expectedLength);
        }
    }
}
