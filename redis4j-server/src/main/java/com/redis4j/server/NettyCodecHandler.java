package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 使用 Netty codec-redis 模块的处理器
 *
 * Pipeline 配置:
 *   RedisDecoder -> RedisMessageAggregator -> RedisEncoder -> NettyCodecHandler
 */
class NettyCodecHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger logger = LoggerFactory.getLogger(NettyCodecHandler.class);

    private final CommandRegistry commandRegistry;
    private final ThreadPoolExecutor commandExecutor;

    public NettyCodecHandler(CommandRegistry commandRegistry, ThreadPoolExecutor commandExecutor) {
        this.commandRegistry = commandRegistry;
        this.commandExecutor = commandExecutor;
    }

    private record ParsedCommand(String name, String[] args) {}

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
        ParsedCommand parsed = parseMessage(msg);
        if (parsed == null) {
            ctx.writeAndFlush(RedisMessageHelper.error("ERR", "protocol error: expected array"));
            return;
        }

        if (parsed.name().isEmpty()) {
            ctx.writeAndFlush(RedisMessageHelper.error("ERR", "empty command"));
            return;
        }

        logger.debug("Executing command: {} {}", parsed.name(), List.of(parsed.args()));

        // 写回操作切回 EventLoop 线程，避免 Netty 内部跨线程调度
        var executor = ctx.executor();
        try {
            commandExecutor.submit(() -> {
                try {
                    RedisMessage response = commandRegistry.execute(parsed.name(), parsed.args());
                    executor.execute(() -> ctx.writeAndFlush(response));
                } catch (Exception e) {
                    logger.error("Error processing command", e);
                    executor.execute(() -> ctx.writeAndFlush(RedisMessageHelper.error("ERR", e.getMessage())));
                }
            });
        } catch (RejectedExecutionException e) {
            ctx.writeAndFlush(RedisMessageHelper.error("ERR", "server is busy, try again later"));
        }
    }

    /**
     * 将 RedisMessage 解析为 (命令名, 参数列表)
     */
    private ParsedCommand parseMessage(RedisMessage msg) {
        if (msg instanceof ArrayRedisMessage array) {
            if (array.isNull()) return null;
            List<RedisMessage> children = array.children();
            if (children.isEmpty()) return new ParsedCommand("", new String[0]);

            String command = extractString(children.get(0)).toUpperCase();
            String[] parameters = new String[children.size() - 1];
            for (int i = 1; i < children.size(); i++) {
                parameters[i - 1] = extractString(children.get(i));
            }
            return new ParsedCommand(command, parameters);
        }

        if (msg instanceof FullBulkStringRedisMessage bulk) {
            if (bulk.isNull()) return null;
            ByteBuf buf = bulk.content();
            if (buf == null || !buf.isReadable()) return new ParsedCommand("", new String[0]);

            String command = buf.toString(StandardCharsets.UTF_8).toUpperCase();
            return new ParsedCommand(command, new String[0]);
        }

        return null;
    }

    private String extractString(RedisMessage msg) {
        if (msg == null) {
            return "";
        }
        if (msg instanceof FullBulkStringRedisMessage bulk) {
            if (bulk.isNull()) return "";
            ByteBuf buf = bulk.content();
            if (buf == null || !buf.isReadable()) return "";
            return buf.toString(StandardCharsets.UTF_8);
        }
        if (msg instanceof SimpleStringRedisMessage s) {
            return s.content();
        }
        if (msg instanceof ErrorRedisMessage err) {
            return err.content();
        }
        if (msg instanceof IntegerRedisMessage i) {
            return String.valueOf(i.value());
        }
        return "";
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in NettyCodecHandler", cause);
        ctx.close();
    }
}
