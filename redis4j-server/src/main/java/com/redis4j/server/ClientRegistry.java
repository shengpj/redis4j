package com.redis4j.server;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

final class ClientRegistry {
    private final AtomicLong nextId = new AtomicLong(1);
    private final ConcurrentHashMap<Channel, ClientState> clients = new ConcurrentHashMap<>();

    void register(Channel channel) {
        clients.computeIfAbsent(channel, ignored -> new ClientState(nextId.getAndIncrement(),
                System.currentTimeMillis(), address(channel.remoteAddress()), address(channel.localAddress())));
    }

    void touch(Channel channel, String command) {
        register(channel);
        ClientState state = clients.get(channel);
        state.lastCommand = command == null ? "" : command.toLowerCase(java.util.Locale.ROOT);
        state.lastCommandAtMillis = System.currentTimeMillis();
    }

    void remove(Channel channel) {
        clients.remove(channel);
    }

    String list(PubSubBroker pubSubBroker) {
        long now = System.currentTimeMillis();
        List<java.util.Map.Entry<Channel, ClientState>> snapshot = clients.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getValue().id))
                .toList();
        StringBuilder result = new StringBuilder();
        for (java.util.Map.Entry<Channel, ClientState> client : snapshot) {
            Channel channel = client.getKey();
            ClientState state = client.getValue();
            if (!channel.isActive()) continue;
            int subscriptions = pubSubBroker.subscriptionCount(channel);
            result.append("id=").append(state.id)
                    .append(" addr=").append(state.remoteAddress)
                    .append(" laddr=").append(state.localAddress)
                    .append(" fd=-1 name= age=").append(seconds(now - state.connectedAtMillis))
                    .append(" idle=").append(seconds(now - state.lastCommandAtMillis))
                    .append(" flags=").append(subscriptions > 0 ? 'P' : 'N')
                    .append(" db=0 sub=").append(subscriptions)
                    .append(" psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=")
                    .append(state.lastCommand)
                    .append('\n');
        }
        return result.toString();
    }

    String address(Channel channel) {
        ClientState state = clients.get(channel);
        return state == null ? address(channel.remoteAddress()) : state.remoteAddress;
    }

    int size() {
        return clients.size();
    }

    private static long seconds(long millis) {
        return Math.max(0, millis / 1000);
    }

    private static String address(SocketAddress address) {
        if (address instanceof InetSocketAddress inet) {
            String host = inet.getAddress() == null ? inet.getHostString() : inet.getAddress().getHostAddress();
            return host + ':' + inet.getPort();
        }
        return address == null ? "unknown" : address.toString().replace(' ', '_');
    }

    private static final class ClientState {
        private final long id;
        private final long connectedAtMillis;
        private final String remoteAddress;
        private final String localAddress;
        private volatile long lastCommandAtMillis;
        private volatile String lastCommand = "";

        private ClientState(long id, long connectedAtMillis, String remoteAddress, String localAddress) {
            this.id = id;
            this.connectedAtMillis = connectedAtMillis;
            this.lastCommandAtMillis = connectedAtMillis;
            this.remoteAddress = remoteAddress;
            this.localAddress = localAddress;
        }
    }
}
