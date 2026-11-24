package io.mq.broker;

/** Runtime knobs the request handler needs (kept free of Spring so the core runs standalone). */
public record BrokerSettings(int minInsyncReplicas, int maxFetchBytes) {
    public static BrokerSettings defaults() { return new BrokerSettings(1, 4 * 1024 * 1024); }
}
