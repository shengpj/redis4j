package com.redis4j.client;

import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.RedisMessageUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/** Redis client based on Netty's RESP codec. */
public class RedisClient {
    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);
    private static final long COMMAND_TIMEOUT_SECONDS = 30;
    private static final int MAX_PENDING_PUBSUB_CALLBACKS = 1024;

    private final String host;
    private final int port;
    private final Queue<PendingRequest> pendingRequests = new ConcurrentLinkedQueue<>();
    private final Set<PendingRequest> outstandingRequests = ConcurrentHashMap.newKeySet();
    private final Set<String> subscriptions = ConcurrentHashMap.newKeySet();

    private volatile EventLoopGroup group;
    private volatile Channel channel;
    private volatile boolean connected;
    private volatile Consumer<PubSubMessage> pubSubListener;
    private volatile ExecutorService pubSubCallbackExecutor;

    public RedisClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RedisClient(String host) {
        this(host, 6379);
    }

    public RedisClient() {
        this("localhost", 6379);
    }

    public synchronized void connect() throws InterruptedException {
        if (isConnected()) {
            return;
        }

        closeCurrentConnection();

        EventLoopGroup newGroup = new NioEventLoopGroup(1);
        group = newGroup;
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(newGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new RedisDecoder());
                        ch.pipeline().addLast(new RedisBulkStringAggregator());
                        ch.pipeline().addLast(new RedisInboundArrayAggregator());
                        ch.pipeline().addLast(new RedisEncoder());
                        ch.pipeline().addLast(new ResponseHandler());
                    }
                });

        ChannelFuture connectFuture = bootstrap.connect(host, port);
        try {
            connectFuture.sync();
            channel = connectFuture.channel();
            pubSubCallbackExecutor = new ThreadPoolExecutor(
                    1, 1, 0, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(MAX_PENDING_PUBSUB_CALLBACKS),
                    r -> {
                        Thread thread = new Thread(r, "redis4j-pubsub-callback");
                        thread.setDaemon(true);
                        return thread;
                    },
                    new ThreadPoolExecutor.AbortPolicy());
            connected = true;
            logger.info("Connected to Redis server at {}:{}", host, port);
        } finally {
            if (!connectFuture.isSuccess()) {
                if (!connectFuture.isDone()) {
                    connectFuture.cancel(true);
                }
                connected = false;
                channel = null;
                newGroup.shutdownGracefully();
                if (group == newGroup) {
                    group = null;
                }
            }
        }
    }

    public synchronized void disconnect() {
        connected = false;
        closeCurrentConnection();
    }

    private void closeCurrentConnection() {
        Channel currentChannel = channel;
        EventLoopGroup currentGroup = group;
        channel = null;
        group = null;
        failPending(new ClosedChannelException());
        clearPubSubState();

        if (currentChannel != null) {
            if (currentChannel.eventLoop().inEventLoop()) {
                currentChannel.close();
            } else {
                currentChannel.close().syncUninterruptibly();
            }
        }
        if (currentGroup != null) {
            if (currentChannel != null && currentChannel.eventLoop().inEventLoop()) {
                currentGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            } else {
                currentGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            }
        }
    }

    public RedisMessage sendCommand(String... args) throws InterruptedException {
        Channel currentChannel = requireActiveChannel();
        if (currentChannel.eventLoop().inEventLoop()) {
            throw new IllegalStateException("Blocking command cannot run on the Netty event loop");
        }

        try {
            return sendCommandFuture(args).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                return null;
            }
            throw new IllegalStateException("Redis command failed", cause);
        }
    }

    public void sendCommandAsync(Consumer<RedisMessage> callback, String... args) {
        if (callback == null) {
            throw new IllegalArgumentException("callback must not be null");
        }
        CompletableFuture<RedisMessage> future;
        try {
            future = sendCommandFuture(args);
        } catch (RuntimeException e) {
            callback.accept(RedisMessageHelper.error("ERR", e.getMessage()));
            return;
        }

        future.whenCompleteAsync((response, error) -> {
            if (error == null) {
                callback.accept(response);
            } else {
                Throwable cause = error instanceof CompletionException && error.getCause() != null
                        ? error.getCause() : error;
                callback.accept(RedisMessageHelper.error("ERR", safeMessage(cause)));
            }
        });
    }

    public CompletableFuture<RedisMessage> sendCommandFuture(String... args) {
        Channel currentChannel = requireActiveChannel();
        if (!subscriptions.isEmpty() && !allowedWhileSubscribed(args)) {
            throw new IllegalStateException(
                    "Only SUBSCRIBE, UNSUBSCRIBE and PING are allowed while this connection is subscribed");
        }
        RedisMessage request = buildArrayMessage(args);
        PendingRequest pending = new PendingRequest();
        outstandingRequests.add(pending);
        if (!connected || channel != currentChannel || !currentChannel.isActive()) {
            ReferenceCountUtil.release(request);
            outstandingRequests.remove(pending);
            pending.future.completeExceptionally(new ClosedChannelException());
            return pending.future;
        }

        try {
            currentChannel.eventLoop().execute(() -> {
                if (pending.future.isDone()) {
                    ReferenceCountUtil.release(request);
                    return;
                }
                if (!currentChannel.isActive()) {
                    ReferenceCountUtil.release(request);
                    outstandingRequests.remove(pending);
                    pending.future.completeExceptionally(new ClosedChannelException());
                    return;
                }

                pendingRequests.add(pending);
                pending.timeout = currentChannel.eventLoop().schedule(
                        () -> timeoutRequest(currentChannel, pending),
                        COMMAND_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS);

                currentChannel.writeAndFlush(request).addListener(writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        failPending(writeFuture.cause());
                        currentChannel.close();
                    }
                });
            });
        } catch (RejectedExecutionException e) {
            ReferenceCountUtil.release(request);
            outstandingRequests.remove(pending);
            pending.future.completeExceptionally(e);
        }
        return pending.future;
    }

    public synchronized int subscribe(Consumer<PubSubMessage> listener, String... channels)
            throws InterruptedException {
        if (listener == null) throw new IllegalArgumentException("listener must not be null");
        if (channels == null || channels.length == 0)
            throw new IllegalArgumentException("at least one channel is required");
        pubSubListener = listener;
        int count = subscriptions.size();
        try {
            for (String topic : channels) {
                if (topic == null) throw new IllegalArgumentException("channel must not be null");
                RedisMessage response = sendCommand("SUBSCRIBE", topic);
                count = subscriptionCount(response, "subscribe", topic);
                subscriptions.add(topic);
            }
            return count;
        } catch (InterruptedException | RuntimeException e) {
            if (subscriptions.isEmpty()) pubSubListener = null;
            throw e;
        }
    }

    public synchronized int unsubscribe(String... channels) throws InterruptedException {
        String[] topics = channels;
        if (topics == null || topics.length == 0) {
            topics = subscriptions.stream().sorted().toArray(String[]::new);
        }
        int count = subscriptions.size();
        for (String topic : topics) {
            RedisMessage response = sendCommand("UNSUBSCRIBE", topic);
            count = subscriptionCount(response, "unsubscribe", topic);
            subscriptions.remove(topic);
        }
        if (count == 0) pubSubListener = null;
        return count;
    }

    public Set<String> getSubscriptions() {
        return Set.copyOf(subscriptions);
    }

    private void timeoutRequest(Channel currentChannel, PendingRequest pending) {
        if (pending.future.isDone()) {
            return;
        }
        TimeoutException timeout = new TimeoutException("command timed out after " + COMMAND_TIMEOUT_SECONDS + " seconds");
        failPending(timeout);
        currentChannel.close();
    }

    private Channel requireActiveChannel() {
        Channel currentChannel = channel;
        if (!connected || currentChannel == null || !currentChannel.isActive()) {
            throw new IllegalStateException("Not connected to Redis server");
        }
        return currentChannel;
    }

    private RedisMessage buildArrayMessage(String... args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("command must not be empty");
        }
        List<RedisMessage> items = new ArrayList<>(args.length);
        for (String arg : args) {
            items.add(RedisMessageHelper.bulkString(arg));
        }
        return new ArrayRedisMessage(items);
    }

    private void completeNext(RedisMessage message) {
        PendingRequest pending = pendingRequests.poll();
        if (pending == null) {
            logger.warn("Received unsolicited Redis response: {}", message.getClass().getSimpleName());
            ReferenceCountUtil.release(message);
            return;
        }
        pending.cancelTimeout();
        outstandingRequests.remove(pending);
        pending.future.complete(message);
    }

    private void failPending(Throwable cause) {
        Throwable failure = cause != null ? cause : new ClosedChannelException();
        for (PendingRequest pending : outstandingRequests) {
            pending.cancelTimeout();
            pending.future.completeExceptionally(failure);
        }
        outstandingRequests.clear();
        pendingRequests.clear();
    }

    private int subscriptionCount(RedisMessage response, String expectedKind, String expectedChannel) {
        List<RedisMessage> values = RedisMessageHelper.extractArray(response);
        if (values == null || values.size() != 3
                || !expectedKind.equalsIgnoreCase(RedisMessageHelper.extractString(values.get(0)))
                || !expectedChannel.equals(RedisMessageHelper.extractString(values.get(1)))) {
            throw new IllegalStateException("invalid " + expectedKind + " response");
        }
        return Math.toIntExact(RedisMessageHelper.extractLong(values.get(2)));
    }

    private static boolean allowedWhileSubscribed(String[] args) {
        if (args == null || args.length == 0 || args[0] == null) return false;
        return "SUBSCRIBE".equalsIgnoreCase(args[0])
                || "UNSUBSCRIBE".equalsIgnoreCase(args[0])
                || "PING".equalsIgnoreCase(args[0]);
    }

    private PubSubMessage pubSubMessage(RedisMessage response) {
        List<RedisMessage> values = RedisMessageHelper.extractArray(response);
        if (values == null || values.size() != 3
                || !"message".equalsIgnoreCase(RedisMessageHelper.extractString(values.get(0)))) return null;
        return new PubSubMessage(RedisMessageHelper.extractString(values.get(1)),
                RedisMessageHelper.extractString(values.get(2)));
    }

    private void dispatchPubSubMessage(PubSubMessage message) {
        Consumer<PubSubMessage> listener = pubSubListener;
        ExecutorService executor = pubSubCallbackExecutor;
        if (listener == null || executor == null) return;
        try {
            executor.execute(() -> {
                try {
                    listener.accept(message);
                } catch (RuntimeException e) {
                    logger.warn("Pub/sub listener failed", e);
                }
            });
        } catch (RejectedExecutionException e) {
            logger.warn("Pub/sub callback queue is full; closing subscription connection");
            Channel currentChannel = channel;
            if (currentChannel != null) currentChannel.close();
        }
    }

    private void clearPubSubState() {
        subscriptions.clear();
        pubSubListener = null;
        ExecutorService executor = pubSubCallbackExecutor;
        pubSubCallbackExecutor = null;
        if (executor != null) executor.shutdownNow();
    }

    private static String safeMessage(Throwable error) {
        String message = error.getMessage();
        return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
    }

    public boolean isConnected() {
        Channel currentChannel = channel;
        return connected && currentChannel != null && currentChannel.isActive();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private final class ResponseHandler extends SimpleChannelInboundHandler<RedisMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
            // Keep the existing application heartbeat behavior unchanged.
            if (msg instanceof SimpleStringRedisMessage simple
                    && "PING".equalsIgnoreCase(simple.content())) {
                ctx.writeAndFlush(RedisMessageHelper.simpleString("PONG"));
                return;
            }
            if (channel != ctx.channel()) {
                return;
            }
            PubSubMessage pubSubMessage = pubSubMessage(msg);
            if (pubSubMessage != null) {
                dispatchPubSubMessage(pubSubMessage);
                return;
            }
            completeNext(RedisMessageUtil.deepCopy(msg));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (channel == ctx.channel()) {
                connected = false;
                channel = null;
                failPending(new ClosedChannelException());
                clearPubSubState();
                logger.info("Disconnected from Redis server");
            }
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (channel == ctx.channel()) {
                failPending(cause);
            }
            ctx.close();
        }
    }

    private static final class PendingRequest {
        private final CompletableFuture<RedisMessage> future = new CompletableFuture<>();
        private ScheduledFuture<?> timeout;

        private void cancelTimeout() {
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }

    public record PubSubMessage(String channel, String payload) {}
}
