package com.redis4j.client;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Inbound aggregator for Redis array responses.
 * Netty's RedisDecoder emits individual messages for each array element.
 * This handler collects them and produces a single ArrayRedisMessage.
 */
public class RedisInboundArrayAggregator extends ChannelDuplexHandler {

    private enum State { IDLE, COLLECTING }

    private State state = State.IDLE;
    private int expectedSize = 0;
    private final List<RedisMessage> elements = new ArrayList<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RedisMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        RedisMessage m = (RedisMessage) msg;

        if (m instanceof ArrayHeaderRedisMessage header) {
            if (header.isNull()) {
                // *-1 null array — emit immediately
                ctx.fireChannelRead(new io.netty.handler.codec.redis.ArrayRedisMessage(java.util.Collections.emptyList()));
                return;
            }
            expectedSize = (int) header.length();
            if (expectedSize == 0) {
                // *0 empty array — emit immediately
                ctx.fireChannelRead(new io.netty.handler.codec.redis.ArrayRedisMessage(new ArrayList<>()));
                return;
            }
            elements.clear();
            state = State.COLLECTING;

        } else if (state == State.COLLECTING) {
            elements.add(m);
            if (elements.size() == expectedSize) {
                ctx.fireChannelRead(new io.netty.handler.codec.redis.ArrayRedisMessage(new ArrayList<>(elements)));
                reset();
            }
        } else {
            ctx.fireChannelRead(m);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.write(msg, promise);
    }

    private void reset() {
        state = State.IDLE;
        expectedSize = 0;
        elements.clear();
    }
}
