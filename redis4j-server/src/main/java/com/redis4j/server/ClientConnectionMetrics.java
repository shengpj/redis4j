package com.redis4j.server;

/** 当前连接限制状态的只读快照。 */
public record ClientConnectionMetrics(int connectedClients, int peakConnectedClients,
                                      long rejectedConnections) {
    public static final ClientConnectionMetrics EMPTY = new ClientConnectionMetrics(0, 0, 0);
}
