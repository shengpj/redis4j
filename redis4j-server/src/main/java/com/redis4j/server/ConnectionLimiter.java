package com.redis4j.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 服务器级连接准入控制器，所有 Channel 共享同一个实例。 */
final class ConnectionLimiter {
    private final int maximumClients;
    private final AtomicInteger connected = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final AtomicLong rejected = new AtomicLong();

    ConnectionLimiter(int maximumClients) {
        if (maximumClients <= 0) throw new IllegalArgumentException("maximumClients must be positive");
        this.maximumClients = maximumClients;
    }

    boolean tryAcquire() {
        while (true) {
            int current = connected.get();
            if (current >= maximumClients) {
                rejected.incrementAndGet();
                return false;
            }
            int updated = current + 1;
            if (connected.compareAndSet(current, updated)) {
                peak.accumulateAndGet(updated, Math::max);
                return true;
            }
        }
    }

    void release() {
        int remaining = connected.decrementAndGet();
        if (remaining < 0) {
            connected.incrementAndGet();
            throw new IllegalStateException("Connection permit released more than once");
        }
    }

    ClientConnectionMetrics snapshot() {
        return new ClientConnectionMetrics(connected.get(), peak.get(), rejected.get());
    }
}
