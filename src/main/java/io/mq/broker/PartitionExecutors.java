package io.mq.broker;

import io.mq.domain.TopicPartition;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** N single-thread executors; a partition always maps to the same one, so its appends never contend. */
public final class PartitionExecutors implements Closeable {
    private final ExecutorService[] pool;

    public PartitionExecutors(int threads) {
        pool = new ExecutorService[threads];
        for (int i = 0; i < threads; i++) {
            String name = "partition-worker-" + i;
            pool[i] = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, name); t.setDaemon(true); return t; });
        }
    }

    public ExecutorService forKey(Object key) { return pool[Math.floorMod(key.hashCode(), pool.length)]; }
    public ExecutorService forPartition(TopicPartition tp) { return forKey(tp); }

    @Override public void close() { for (ExecutorService e : pool) e.shutdownNow(); }
}
