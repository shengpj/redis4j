package com.redis4j.server;

import com.redis4j.protocol.RedisMessageHelper;
import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class PubSubBroker {
    private final ConcurrentHashMap<String, Set<Channel>> subscribers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Channel, Set<String>> subscriptions = new ConcurrentHashMap<>();

    synchronized int subscribe(Channel channel, String topic) {
        subscriptions.computeIfAbsent(channel, ignored -> ConcurrentHashMap.newKeySet()).add(topic);
        subscribers.computeIfAbsent(topic, ignored -> ConcurrentHashMap.newKeySet()).add(channel);
        return subscriptions.get(channel).size();
    }

    synchronized int unsubscribe(Channel channel, String topic) {
        Set<String> channelTopics = subscriptions.get(channel);
        if (channelTopics != null) {
            channelTopics.remove(topic);
            if (channelTopics.isEmpty()) subscriptions.remove(channel);
        }
        Set<Channel> topicSubscribers = subscribers.get(topic);
        if (topicSubscribers != null) {
            topicSubscribers.remove(channel);
            if (topicSubscribers.isEmpty()) subscribers.remove(topic);
        }
        return subscriptionCount(channel);
    }

    synchronized void remove(Channel channel) {
        Set<String> topics = subscriptions.remove(channel);
        if (topics == null) return;
        for (String topic : topics) {
            Set<Channel> topicSubscribers = subscribers.get(topic);
            if (topicSubscribers == null) continue;
            topicSubscribers.remove(channel);
            if (topicSubscribers.isEmpty()) subscribers.remove(topic);
        }
    }

    int publish(String topic, String payload) {
        Set<Channel> current = subscribers.get(topic);
        if (current == null || current.isEmpty()) return 0;

        int delivered = 0;
        for (Channel channel : Set.copyOf(current)) {
            if (!channel.isActive() || !channel.isWritable()) {
                if (channel.isActive()) channel.close();
                remove(channel);
                continue;
            }
            channel.writeAndFlush(RedisMessageHelper.array(
                    RedisMessageHelper.bulkString("message"),
                    RedisMessageHelper.bulkString(topic),
                    RedisMessageHelper.bulkString(payload)));
            delivered++;
        }
        return delivered;
    }

    boolean isSubscribed(Channel channel) {
        return subscriptionCount(channel) > 0;
    }

    int subscriptionCount(Channel channel) {
        Set<String> topics = subscriptions.get(channel);
        return topics == null ? 0 : topics.size();
    }

    Set<String> subscriptions(Channel channel) {
        Set<String> topics = subscriptions.get(channel);
        return topics == null ? Set.of() : Set.copyOf(topics);
    }
}
