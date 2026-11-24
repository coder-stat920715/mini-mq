package io.mq.client;

import io.mq.domain.Acks;
import io.mq.domain.Api;
import io.mq.domain.Errors;
import io.mq.domain.TopicPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batching producer. Records are encoded straight into per-partition batches backed by a fixed pool of
 * direct buffers (bounded memory; {@code send} blocks up to maxBlockMs when the pool is exhausted =
 * back-pressure). One sender thread flushes batches that are full or older than lingerMs. With
 * idempotence on, every batch carries (producerId, epoch, baseSequence); retries reuse the same
 * sequence so the broker de-duplicates them: no duplicates, no reordering.
 */
public final class MqProducer implements AutoCloseable {

    public record Config(String host, int port, Acks acks, int batchBytes, long bufferBytes, int lingerMs,
                         int maxRetries, boolean idempotent, long maxBlockMs, String transactionalId) {
        public static Config defaults(String host, int port) {
            return new Config(host, port, Acks.ALL, 64 * 1024, 32L * 1024 * 1024, 5, 5, true, 10_000, "");
        }
    }

    public record RecordMetadata(TopicPartition tp, long offset) {}

    private static final class Batch {
        final TopicPartition tp; final ByteBuffer buf; final boolean pooled;
        final long createdNanos = System.nanoTime();
        final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        int count; boolean sealed;
        Batch(TopicPartition tp, ByteBuffer buf, boolean pooled) { this.tp = tp; this.buf = buf; this.pooled = pooled; }
    }

    private final Config cfg;
    private final BlockingQueue<ByteBuffer> pool;
    private final Map<TopicPartition, ArrayDeque<Batch>> batches = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Integer> nextSeq = new ConcurrentHashMap<>();   // sender thread only
    private final BrokerConnection dataConn, metaConn;
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final AtomicLong pending = new AtomicLong();
    private final Object wake = new Object();
    private final Thread sender;
    private volatile boolean running = true, flushing;
    private volatile long producerId = -1;
    private volatile short epoch;

    public MqProducer(Config cfg) {
        this.cfg = cfg;
        int n = (int) Math.max(2, cfg.bufferBytes() / cfg.batchBytes());
        pool = new ArrayBlockingQueue<>(n);
        for (int i = 0; i < n; i++) pool.add(ByteBuffer.allocateDirect(cfg.batchBytes()));
        dataConn = new BrokerConnection(cfg.host(), cfg.port());
        metaConn = new BrokerConnection(cfg.host(), cfg.port());
        sender = new Thread(this::senderLoop, "mq-producer-sender");
        sender.setDaemon(true);
        sender.start();
    }

    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        return send(topic, -1, key, value);
    }

    public CompletableFuture<RecordMetadata> send(String topic, int partition, byte[] key, byte[] value) {
        if (!running) throw new IllegalStateException("producer closed");
        int parts = partitionsFor(topic);
        int p = partition >= 0 ? partition : partitionFor(key, parts);
        if (p >= parts) throw new IllegalArgumentException("partition " + p + " >= " + parts);
        TopicPartition tp = new TopicPartition(topic, p);
        int need = 8 + 4 + (key == null ? 0 : key.length) + 4 + (value == null ? 0 : value.length);
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        ArrayDeque<Batch> dq = batches.computeIfAbsent(tp, k -> new ArrayDeque<>());
        ByteBuffer spare = null;                                   // acquired outside the lock: no deadlock with the sender
        while (true) {
            synchronized (dq) {
                Batch b = dq.peekLast();
                if (b != null && !b.sealed && b.buf.remaining() >= need) {
                    encode(b.buf, key, value);
                    b.count++; b.futures.add(f);
                    if (b.buf.remaining() < 64) b.sealed = true;
                    pending.incrementAndGet();
                    if (spare != null) pool.offer(spare);
                    break;
                }
                if (spare != null) {
                    if (b != null) b.sealed = true;
                    Batch nb = new Batch(tp, spare, need <= cfg.batchBytes());
                    dq.addLast(nb);
                    encode(nb.buf, key, value);
                    nb.count++; nb.futures.add(f);
                    pending.incrementAndGet();
                    break;
                }
            }
            spare = need <= cfg.batchBytes() ? acquire() : ByteBuffer.allocate(need);   // oversized record: own buffer
            spare.clear();
        }
        synchronized (wake) { wake.notifyAll(); }
        return f;
    }

    private ByteBuffer acquire() {
        try {
            ByteBuffer b = pool.poll(cfg.maxBlockMs(), TimeUnit.MILLISECONDS);
            if (b == null) throw new ClientException("buffer pool exhausted after " + cfg.maxBlockMs() + " ms", null);
            return b;
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new ClientException("interrupted", e); }
    }

    private static void encode(ByteBuffer b, byte[] key, byte[] value) {
        b.putLong(System.currentTimeMillis());
        b.putInt(key == null ? -1 : key.length); if (key != null) b.put(key);
        b.putInt(value == null ? -1 : value.length); if (value != null) b.put(value);
    }

    private int partitionFor(byte[] key, int parts) {
        if (key == null) return Math.floorMod(roundRobin.getAndIncrement(), parts);
        int h = java.util.Arrays.hashCode(key);
        h ^= (h >>> 16); h *= 0x85ebca6b; h ^= (h >>> 13);
        return Math.floorMod(h, parts);
    }

    private int partitionsFor(String topic) {
        Integer n = partitionCounts.get(topic);
        if (n != null) return n;
        try {
            var r = metaConn.call(Api.METADATA, (ByteBuffer) ByteBuffer.allocate(4 + BrokerConnection.strSize(topic))
                    .putInt(1).put(BrokerConnection.str(topic)).flip());
            ByteBuffer body = r.body();
            int count = body.getInt();
            for (int i = 0; i < count; i++) partitionCounts.put(BrokerConnection.getStr(body), body.getInt());
        } catch (IOException e) { throw new ClientException("metadata failed", e); }
        n = partitionCounts.get(topic);
        if (n == null) throw new ClientException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic);
        return n;
    }

    /** Blocks until every record sent so far has been acknowledged (or failed). */
    public void flush() {
        flushing = true;
        try {
            synchronized (wake) { wake.notifyAll(); }
            while (pending.get() > 0) Thread.sleep(1);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { flushing = false; }
    }

    // ---- sender thread --------------------------------------------------------------------------------

    private void senderLoop() {
        while (running || pending.get() > 0) {
            boolean sent = false;
            for (var e : batches.entrySet()) {
                Batch b;
                ArrayDeque<Batch> dq = e.getValue();
                synchronized (dq) {
                    b = dq.peekFirst();
                    if (b == null) continue;
                    boolean lingered = (System.nanoTime() - b.createdNanos) / 1_000_000 >= cfg.lingerMs();
                    if (!(b.sealed || lingered || flushing || !running)) continue;
                    dq.pollFirst();
                    b.sealed = true;
                }
                transmit(b);
                sent = true;
            }
            if (!sent) {
                synchronized (wake) {
                    try { wake.wait(Math.max(1, cfg.lingerMs())); } catch (InterruptedException ie) { return; }
                }
            }
        }
    }

    private void transmit(Batch b) {
        b.buf.flip();
        try {
            if (cfg.idempotent() && producerId < 0) initProducerId();
            int baseSeq = nextSeq.getOrDefault(b.tp, 0);
            ByteBuffer hdr = header(b, baseSeq);
            Throwable last = null;
            for (int attempt = 0; attempt <= cfg.maxRetries(); attempt++) {
                if (attempt > 0) Thread.sleep(Math.min(1000, 20L << attempt));
                try {
                    if (cfg.acks() == Acks.NONE) {
                        dataConn.send(Api.PRODUCE, hdr.duplicate(), b.buf.duplicate());
                        complete(b, -1);
                        return;
                    }
                    var r = dataConn.call(Api.PRODUCE, hdr.duplicate(), b.buf.duplicate());
                    if (r.error() == Errors.NONE) {
                        if (cfg.idempotent()) nextSeq.put(b.tp, baseSeq + b.count);
                        complete(b, r.body().getLong());
                        return;
                    }
                    last = new ClientException(r.error(), "produce to " + b.tp);
                    if (!Errors.retriable(r.error())) break;
                } catch (IOException ioe) { last = ioe; }        // ambiguous outcome: retry with the SAME sequence
            }
            fail(b, last);
            if (cfg.idempotent()) { producerId = -1; nextSeq.clear(); }   // sequence state is now uncertain: start fresh
        } catch (Exception e) {
            fail(b, e);
        } finally {
            if (b.pooled) pool.offer(b.buf);
        }
    }

    private ByteBuffer header(Batch b, int baseSeq) {
        ByteBuffer h = ByteBuffer.allocate(BrokerConnection.strSize(b.tp.topic()) + 4 + 8 + 2 + 4 + 1 + 4);
        BrokerConnection.putStr(h, b.tp.topic());
        h.putInt(b.tp.partition());
        h.putLong(cfg.idempotent() ? producerId : -1L).putShort(epoch).putInt(baseSeq);
        h.put(cfg.acks().code).putInt(b.count);
        h.flip();
        return h;
    }

    private void initProducerId() throws IOException {
        var r = dataConn.call(Api.INIT_PRODUCER_ID, BrokerConnection.str(cfg.transactionalId() == null ? "" : cfg.transactionalId()));
        if (r.error() != Errors.NONE) throw new ClientException(r.error(), "initProducerId");
        producerId = r.body().getLong();
        epoch = r.body().getShort();
    }

    private void complete(Batch b, long base) {
        for (int i = 0; i < b.futures.size(); i++)
            b.futures.get(i).complete(new RecordMetadata(b.tp, base < 0 ? -1 : base + i));
        pending.addAndGet(-b.count);
    }

    private void fail(Batch b, Throwable t) {
        ClientException ce = t instanceof ClientException c ? c : new ClientException("produce failed", t);
        b.futures.forEach(f -> f.completeExceptionally(ce));
        pending.addAndGet(-b.count);
    }

    @Override public void close() {
        if (!running) return;
        flush();
        running = false;
        synchronized (wake) { wake.notifyAll(); }
        try { sender.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        dataConn.close(); metaConn.close();
    }
}
