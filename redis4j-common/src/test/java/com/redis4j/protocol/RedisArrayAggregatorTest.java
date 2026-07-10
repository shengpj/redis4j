package com.redis4j.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedisArrayAggregatorTest {

    @Test
    void aggregatesFragmentedBulkStringsAndPreservesEmptyString() {
        EmbeddedChannel channel = newChannel();
        byte[] payload = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n"
                .getBytes(StandardCharsets.UTF_8);

        writeFragmented(channel, payload, 2);
        ArrayRedisMessage array = channel.readInbound();
        assertNotNull(array);
        assertEquals(3, array.children().size());
        FullBulkStringRedisMessage empty = (FullBulkStringRedisMessage) array.children().get(2);
        assertFalse(empty.isNull());
        assertEquals(0, empty.content().readableBytes());

        ReferenceCountUtil.release(array);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void aggregatesLargeBulkStringSplitAcrossManyBuffers() {
        EmbeddedChannel channel = newChannel();
        String value = "x".repeat(32_768);
        byte[] payload = ("*2\r\n$3\r\nSET\r\n$" + value.length() + "\r\n" + value + "\r\n")
                .getBytes(StandardCharsets.UTF_8);

        writeFragmented(channel, payload, 113);
        ArrayRedisMessage array = channel.readInbound();
        assertNotNull(array);
        FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) array.children().get(1);
        assertEquals(value, bulk.content().toString(StandardCharsets.UTF_8));

        ReferenceCountUtil.release(array);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void supportsNestedAndNullArrays() {
        EmbeddedChannel channel = newChannel();
        byte[] payload = "*2\r\n*2\r\n:1\r\n:2\r\n*-1\r\n".getBytes(StandardCharsets.UTF_8);

        writeFragmented(channel, payload, 3);
        ArrayRedisMessage outer = channel.readInbound();
        assertNotNull(outer);
        assertEquals(2, outer.children().size());

        ArrayRedisMessage nested = (ArrayRedisMessage) outer.children().get(0);
        assertEquals(List.of(1L, 2L), nested.children().stream()
                .map(message -> ((IntegerRedisMessage) message).value())
                .toList());
        assertTrue(((ArrayRedisMessage) outer.children().get(1)).isNull());

        ReferenceCountUtil.release(outer);
        assertFalse(channel.finishAndReleaseAll());
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisDecoder(),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator());
    }

    private static void writeFragmented(EmbeddedChannel channel, byte[] payload, int fragmentSize) {
        for (int offset = 0; offset < payload.length; offset += fragmentSize) {
            int length = Math.min(fragmentSize, payload.length - offset);
            ByteBuf fragment = Unpooled.copiedBuffer(payload, offset, length);
            channel.writeInbound(fragment);
        }
    }
}
