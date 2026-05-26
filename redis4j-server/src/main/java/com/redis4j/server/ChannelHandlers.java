package com.redis4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Netty Channel 处理工具类
 */
public final class ChannelHandlers {

    public static final ByteBuf CRLF_DELIMITER = Unpooled.copiedBuffer("\r\n", java.nio.charset.StandardCharsets.UTF_8);
    public static final ByteBuf LF_DELIMITER = Unpooled.copiedBuffer("\n", java.nio.charset.StandardCharsets.UTF_8);

    private ChannelHandlers() {
    }
}
