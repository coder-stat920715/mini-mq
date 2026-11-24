package io.mq.broker;

import io.mq.storage.LogManager;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Wires the broker core together. Framework-free: Spring (or a test) just constructs and closes it. */
public final class Broker implements Closeable {

    public record Config(Path dataDir, int port, int ioThreads, int partitionThreads,
                         long segmentBytes, int indexIntervalBytes, long retentionMs, long retentionBytes,
                         long flushIntervalMs, int minInsyncReplicas, GroupCoordinator.Strategy strategy,
                         int maxFetchBytes) {
        public static Config defaults(Path dataDir, int port) {
            return new Config(dataDir, port, Math.max(2, Runtime.getRuntime().availableProcessors()),
                    Math.max(2, Runtime.getRuntime().availableProcessors()),
                    256L * 1024 * 1024, 4096, 7L * 24 * 3600 * 1000, -1, 1000, 1,
                    GroupCoordinator.Strategy.STICKY, 4 * 1024 * 1024);
        }
        public Config withSegmentBytes(long v) {
            return new Config(dataDir, port, ioThreads, partitionThreads, v, indexIntervalBytes, retentionMs, retentionBytes,
                    flushIntervalMs, minInsyncReplicas, strategy, maxFetchBytes);
        }
    }

    private final LogManager logs;
    private final GroupCoordinator groups;
    private final OffsetStore offsets;
    private final PartitionExecutors executors;
    private final RequestHandler handler;
    private final BrokerServer server;
    private final ScheduledExecutorService sessions =
            Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "group-session-expiry"); t.setDaemon(true); return t; });

    public Broker(Config c) throws IOException {
        logs = new LogManager(new LogManager.Settings(c.dataDir(), c.segmentBytes(), c.indexIntervalBytes(),
                c.retentionMs(), c.retentionBytes(), c.flushIntervalMs()));
        offsets = new OffsetStore(logs);
        groups = new GroupCoordinator(t -> Math.max(0, logs.partitions(t)), c.strategy());
        executors = new PartitionExecutors(c.partitionThreads());
        handler = new RequestHandler(logs, groups, offsets, new ProducerStateManager(), executors,
                new BrokerSettings(c.minInsyncReplicas(), c.maxFetchBytes()));
        server = new BrokerServer(handler, c.port(), c.ioThreads());
        sessions.scheduleWithFixedDelay(groups::expireSessions, 1, 1, TimeUnit.SECONDS);
    }

    public Broker start() throws InterruptedException { server.start(); return this; }
    public int port() { return server.port(); }
    public LogManager logs() { return logs; }
    public GroupCoordinator groups() { return groups; }
    public RequestHandler handler() { return handler; }

    @Override public void close() {
        sessions.shutdownNow();
        server.close();
        executors.close();
        try { logs.close(); } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
    }
}
