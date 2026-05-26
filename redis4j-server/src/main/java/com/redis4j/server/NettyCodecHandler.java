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
import java.util.concurrent.ExecutorService;

/**
 * 使用 Netty codec-redis 模块的处理器
 *
 * Pipeline 配置:
 *   RedisDecoder -> RedisMessageAggregator -> RedisEncoder -> NettyCodecHandler
 */
class NettyCodecHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger logger = LoggerFactory.getLogger(NettyCodecHandler.class);

    private final CommandRegistry commandRegistry;
    private final ExecutorService commandExecutor;

    public NettyCodecHandler(CommandRegistry commandRegistry, ExecutorService commandExecutor) {
        this.commandRegistry = commandRegistry;
        this.commandExecutor = commandExecutor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Client connected: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Client disconnected: {}", ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        System.err.println(">>> [NettyCodecHandler] Received: " + msg.getClass().getSimpleName());

        // 处理 ArrayRedisMessage（由聚合器输出）
        if (msg instanceof ArrayRedisMessage) {
            ArrayRedisMessage array = (ArrayRedisMessage) msg;
            if (array.isNull()) {
                ctx.writeAndFlush(RedisMessageHelper.error("ERR", "empty command"));
                return;
            }

            List<RedisMessage> children = array.children();
            System.err.println(">>> [NettyCodecHandler] Array children count: " + children.size());
            for (int i = 0; i < children.size(); i++) {
                RedisMessage child = children.get(i);
                System.err.println(">>> [NettyCodecHandler] children[" + i + "] = " + child.getClass().getSimpleName() + " -> " + child);
            }

            if (children.isEmpty()) {
                ctx.writeAndFlush(RedisMessageHelper.error("ERR", "empty command"));
                return;
            }

            // 解析命令
            RedisMessage cmdMsg = children.get(0);
            String command = extractString(cmdMsg).toUpperCase();

            // 提取参数
            String[] parameters = new String[children.size() - 1];
            for (int i = 1; i < children.size(); i++) {
                parameters[i - 1] = extractString(children.get(i));
            }

            logger.debug("Executing command: {} {}", command, List.of(parameters));

            // 提交到线程池处理
            final String cmd = command;
            final String[] params = parameters;
            commandExecutor.submit(() -> {
                try {
                    RedisMessage response = commandRegistry.execute(cmd, params);
                    ctx.writeAndFlush(response);
                } catch (Exception e) {
                    logger.error("Error processing command", e);
                    ctx.writeAndFlush(RedisMessageHelper.error("ERR", e.getMessage()));
                }
            });
            return;
        }

        // 处理直接的 BulkString（用于 PING 等简单命令）
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) {
                ctx.writeAndFlush(RedisMessageHelper.error("ERR", "empty command"));
                return;
            }

            ByteBuf buf = bulk.content();
            if (buf == null || !buf.isReadable()) {
                ctx.writeAndFlush(RedisMessageHelper.error("ERR", "empty command"));
                return;
            }

            String command = buf.toString(StandardCharsets.UTF_8).toUpperCase();
            logger.debug("Executing command: {}", command);

            final String cmd = command;
            commandExecutor.submit(() -> {
                try {
                    RedisMessage response = commandRegistry.execute(cmd, new String[0]);
                    ctx.writeAndFlush(response);
                } catch (Exception e) {
                    logger.error("Error processing command", e);
                    ctx.writeAndFlush(RedisMessageHelper.error("ERR", e.getMessage()));
                }
            });
            return;
        }

        logger.warn("Received unexpected message type: {}", msg.getClass().getSimpleName());
        ctx.writeAndFlush(RedisMessageHelper.error("ERR", "protocol error: expected array"));
    }

    private String extractString(RedisMessage msg) {
        if (msg == null) {
            return "";
        }
        if (msg instanceof FullBulkStringRedisMessage) {
            FullBulkStringRedisMessage bulk = (FullBulkStringRedisMessage) msg;
            if (bulk.isNull()) {
                return "";
            }
            ByteBuf buf = bulk.content();
            if (buf == null || !buf.isReadable()) {
                return "";
            }
            return buf.toString(StandardCharsets.UTF_8);
        }
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        }
        if (msg instanceof ErrorRedisMessage) {
            return ((ErrorRedisMessage) msg).content();
        }
        if (msg instanceof IntegerRedisMessage) {
            return String.valueOf(((IntegerRedisMessage) msg).value());
        }
        return "";
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in NettyCodecHandler", cause);
        ctx.close();
    }
}
