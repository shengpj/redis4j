package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.response.CommandResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.*;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
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
    private final Deque<ParsedCommand> pendingCommands = new ArrayDeque<>();
    private boolean commandRunning;

    private static final int MAX_PENDING_COMMANDS_PER_CONNECTION = 1024;

    public NettyCodecHandler(CommandRegistry commandRegistry, ThreadPoolExecutor commandExecutor) {
        this.commandRegistry = commandRegistry;
        this.commandExecutor = commandExecutor;
    }

    private volatile boolean heartbeatPending;

    private record ParsedCommand(String name, String[] args) {}

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
        // 收到任何数据都说明客户端活着，清除心跳标记
        heartbeatPending = false;

        // 服务端心跳 PONG 应答，不往下处理
        if (msg instanceof SimpleStringRedisMessage s && "PONG".equalsIgnoreCase(s.content())) {
            logger.trace("Received heartbeat PONG from {}", ctx.channel().remoteAddress());
            return;
        }

        ParsedCommand parsed = parseMessage(msg);
        if (parsed == null) {
            ctx.writeAndFlush(RedisMessageHelper.error("ERR protocol error: expected array"));
            return;
        }

        if (parsed.name().isEmpty()) {
            ctx.writeAndFlush(RedisMessageHelper.error("ERR empty command"));
            return;
        }

        logger.debug("Executing command: {} {}", parsed.name(), List.of(parsed.args()));

        // 写回操作切回 EventLoop 线程，避免 Netty 内部跨线程调度
        if (pendingCommands.size() >= MAX_PENDING_COMMANDS_PER_CONNECTION) {
            logger.warn("Too many pending commands, closing: {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        pendingCommands.addLast(parsed);
        dispatchNext(ctx);
    }

    private void dispatchNext(ChannelHandlerContext ctx) {
        if (commandRunning) {
            return;
        }
        ParsedCommand parsed = pendingCommands.pollFirst();
        if (parsed == null) {
            return;
        }
        commandRunning = true;
        try {
            commandExecutor.submit(() -> {
                CommandResponse response;
                try {
                    response = commandRegistry.execute(parsed.name(), parsed.args());
                } catch (Exception e) {
                    logger.error("Error processing command", e);
                    response = new com.redis4j.protocol.response.CommandResponse.Error("ERR " + e.getMessage());
                }
                RedisMessage completedResponse = NettyResponseAdapter.adapt(response);
                try {
                    ctx.executor().execute(() -> completeCommand(ctx, completedResponse));
                } catch (RejectedExecutionException e) {
                    io.netty.util.ReferenceCountUtil.release(completedResponse);
                }
            });
        } catch (RejectedExecutionException e) {
            completeCommand(ctx, RedisMessageHelper.error("ERR server is busy, try again later"));
        }
    }

    private void completeCommand(ChannelHandlerContext ctx, RedisMessage response) {
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            io.netty.util.ReferenceCountUtil.release(response);
        }
        commandRunning = false;
        dispatchNext(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        pendingCommands.clear();
        super.channelInactive(ctx);
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

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            if (heartbeatPending) {
                // 上次心跳无回应 → 断开
                logger.warn("No heartbeat response, closing: {}", ctx.channel().remoteAddress());
                ctx.close();
            } else {
                // 30 秒无读事件，发 PING 探活
                heartbeatPending = true;
                logger.debug("Sending heartbeat ping to {}", ctx.channel().remoteAddress());
                ctx.writeAndFlush(RedisMessageHelper.simpleString("PING"));
            }
        }
    }
}
