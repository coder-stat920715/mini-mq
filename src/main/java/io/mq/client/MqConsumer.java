package io.mq.client;

import io.mq.domain.Acks;
import io.mq.domain.Api;
import io.mq.domain.Errors;
import io.mq.domain.TopicPartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32C;

/**
 * Group consumer: join/heartbeat/rebalance, pipelined-free simple fetch loop, offset commit, and
 * queue-style ack/nack with exponential-backoff redelivery and a dead-letter topic ("&lt;topic&gt;.DLQ").
 *
 * Not thread-safe except {@link #close()}: call poll/commit/ack/nack from one thread. A background thread
 * sends heartbeats so slow record processing does not expire the session.
 */
public final class MqConsumer implements AutoCloseable {

    public record Config(String host, int port, String groupId, boolean autoCommit, long autoCommitIntervalMs,
                         long sessionTimeoutMs, long heartbeatIntervalMs, int maxFetchBytes,
                         String autoOffsetReset, int maxRetries, long retryBackoffMs) {
        public static Config defaults(String host, int port, String groupId) {
            return new Config(host, port, groupId, true, 1000, 10_000, 1000, 1 << 20, "earliest", 3, 200);
        }
        public Config withAutoCommit(boolean v) {
            return new Config(host, port, groupId, v, autoCommitIntervalMs, sessionTimeoutMs, heartbeatIntervalMs, maxFetchBytes, autoOffsetReset, maxRetries, retryBackoffMs);
        }
    }

    private record Delayed(long dueMs, ConsumerRecord rec) {}

    private final Config cfg;
    private final BrokerConnection conn, hbConn;
    private final Thread heartbeat;
    private volatile boolean running = true, needsRejoin = true;
    private volatile String memberId = "";
    private volatile int generation = -1;

    private List<String> topics = List.of();
    private RebalanceListener listener = RebalanceListener.NOOP;
    private volatile List<TopicPartition> assignment = List.of();
    private final Map<TopicPartition, Long> position = new HashMap<>();
    private final Map<TopicPartition, Long> lastCommitted = new HashMap<>();
    private final Map<TopicPartition, TreeSet<Long>> outstanding = new HashMap<>();   // nacked, not yet resolved
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
    private final PriorityQueue<Delayed> delayed = new PriorityQueue<>(Comparator.comparingLong(Delayed::dueMs));
    private long lastAutoCommit = System.currentTimeMillis();
    private MqProducer dlqProducer;

    public MqConsumer(Config cfg) {
        this.cfg = cfg;
        this.conn = new BrokerConnection(cfg.host(), cfg.port());
        this.hbConn = new BrokerConnection(cfg.host(), cfg.port());
        this.heartbeat = new Thread(this::heartbeatLoop, "mq-consumer-heartbeat-" + cfg.groupId());
        this.heartbeat.setDaemon(true);
        this.heartbeat.start();
    }

    public void subscribe(Collection<String> topics, RebalanceListener listener) {
        this.topics = List.copyOf(topics);
        this.listener = listener == null ? RebalanceListener.NOOP : listener;
        this.needsRejoin = true;
    }

    public List<TopicPartition> assignment() { return assignment; }
    public String memberId() { return memberId; }
    public int generation() { return generation; }

    // ---- poll ----------------------------------------------------------------------------------------

    public List<ConsumerRecord> poll(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        if (needsRejoin) rejoin();
        List<ConsumerRecord> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        while (!delayed.isEmpty() && delayed.peek().dueMs() <= now) out.add(delayed.poll().rec());
        List<TopicPartition> parts = assignment;
        if (parts.isEmpty()) { sleep(Math.min(50, Math.max(1, timeout.toMillis()))); return out; }
        for (TopicPartition tp : parts) fetch(tp, 0, out);
        while (out.isEmpty() && System.currentTimeMillis() < deadline && !needsRejoin && running) {
            long per = Math.max(5, (deadline - System.currentTimeMillis()) / parts.size());
            for (TopicPartition tp : parts) { fetch(tp, (int) per, out); if (!out.isEmpty()) break; }
            long due = delayed.isEmpty() ? Long.MAX_VALUE : delayed.peek().dueMs();
            if (due <= System.currentTimeMillis()) { out.add(delayed.poll().rec()); }
        }
        if (cfg.autoCommit() && System.currentTimeMillis() - lastAutoCommit >= cfg.autoCommitIntervalMs()) {
            try { commitSync(); } catch (ClientException ignored) { /* retried next interval */ }
        }
        return out;
    }

    private void fetch(TopicPartition tp, int waitMs, List<ConsumerRecord> out) {
        long pos = position.getOrDefault(tp, cfg.autoOffsetReset().equals("latest") ? Api.LATEST : Api.EARLIEST);
        ByteBuffer req = ByteBuffer.allocate(BrokerConnection.strSize(tp.topic()) + 4 + 8 + 4 + 4);
        BrokerConnection.putStr(req, tp.topic());
        req.putInt(tp.partition()).putLong(pos).putInt(cfg.maxFetchBytes()).putInt(waitMs).flip();
        try {
            var r = conn.call(Api.FETCH, req);
            if (r.error() == Errors.OFFSET_OUT_OF_RANGE) {
                position.put(tp, cfg.autoOffsetReset().equals("latest") ? Api.LATEST : Api.EARLIEST);
                return;
            }
            if (r.error() != Errors.NONE) throw new ClientException(r.error(), "fetch " + tp);
            ByteBuffer b = r.body();
            b.getLong();                                   // high watermark
            long next = b.getLong();
            int dataLen = b.getInt();
            int end = b.position() + dataLen;
            while (b.position() < end) {
                long offset = b.getLong();
                int size = b.getInt();
                int crc = b.getInt();
                CRC32C c = new CRC32C();
                c.update(b.duplicate().limit(b.position() + size - 4));
                if ((int) c.getValue() != crc) throw new ClientException("corrupt record at " + tp + "@" + offset, null);
                long ts = b.getLong();
                int kl = b.getInt(); byte[] k = null;
                if (kl >= 0) { k = new byte[kl]; b.get(k); }
                int vl = b.getInt(); byte[] v = null;
                if (vl >= 0) { v = new byte[vl]; b.get(v); }
                out.add(new ConsumerRecord(tp, offset, ts, k, v, 1));
            }
            position.put(tp, next);
        } catch (IOException e) { throw new ClientException("fetch failed", e); }
    }

    // ---- ack / nack / DLQ ----------------------------------------------------------------------------

    /** Marks a (possibly redelivered) record as fully processed. */
    public void ack(ConsumerRecord r) {
        attempts.remove(key(r));
        var set = outstanding.get(r.tp());
        if (set != null) set.remove(r.offset());
    }

    /** Negative ack: redeliver after exponential backoff; after maxRetries the record goes to "&lt;topic&gt;.DLQ". */
    public void nack(ConsumerRecord r) {
        int n = attempts.merge(key(r), 1, Integer::sum);
        outstanding.computeIfAbsent(r.tp(), k -> new TreeSet<>()).add(r.offset());
        if (n > cfg.maxRetries()) {
            sendToDlq(r);
            ack(r);
            return;
        }
        long delay = Math.min(60_000, cfg.retryBackoffMs() << (n - 1));
        delayed.add(new Delayed(System.currentTimeMillis() + delay,
                new ConsumerRecord(r.tp(), r.offset(), r.timestamp(), r.key(), r.value(), n + 1)));
    }

    private void sendToDlq(ConsumerRecord r) {
        String dlq = r.tp().topic() + ".DLQ";
        try (Admin admin = new Admin(cfg.host(), cfg.port())) { admin.createTopic(dlq, 1); }
        if (dlqProducer == null) {
            dlqProducer = new MqProducer(new MqProducer.Config(cfg.host(), cfg.port(), Acks.ALL, 16 * 1024, 1 << 20, 1, 5, false, 5000, ""));
        }
        try { dlqProducer.send(dlq, r.key(), r.value()).get(); }
        catch (Exception e) { throw new ClientException("DLQ publish failed", e); }
    }

    private static String key(ConsumerRecord r) { return r.tp() + "@" + r.offset(); }

    // ---- commit --------------------------------------------------------------------------------------

    /** Commits current positions, but never past a record that is still waiting for redelivery. */
    public void commitSync() {
        Map<TopicPartition, Long> toCommit = new HashMap<>();
        for (var e : position.entrySet()) {
            long pos = e.getValue();
            if (pos < 0) continue;                                       // not yet resolved
            var pend = outstanding.get(e.getKey());
            if (pend != null && !pend.isEmpty()) pos = Math.min(pos, pend.first());
            if (!Long.valueOf(pos).equals(lastCommitted.get(e.getKey()))) toCommit.put(e.getKey(), pos);
        }
        lastAutoCommit = System.currentTimeMillis();
        if (toCommit.isEmpty() || memberId.isEmpty()) return;
        int size = BrokerConnection.strSize(cfg.groupId()) + 4 + BrokerConnection.strSize(memberId) + 4;
        for (TopicPartition tp : toCommit.keySet()) size += BrokerConnection.strSize(tp.topic()) + 4 + 8;
        ByteBuffer req = ByteBuffer.allocate(size);
        BrokerConnection.putStr(req, cfg.groupId());
        req.putInt(generation);
        BrokerConnection.putStr(req, memberId);
        req.putInt(toCommit.size());
        toCommit.forEach((tp, off) -> { BrokerConnection.putStr(req, tp.topic()); req.putInt(tp.partition()).putLong(off); });
        req.flip();
        try {
            var r = conn.call(Api.COMMIT_OFFSET, req);
            if (r.error() == Errors.ILLEGAL_GENERATION || r.error() == Errors.UNKNOWN_MEMBER_ID) { needsRejoin = true; throw new ClientException(r.error(), "commit rejected (stale generation)"); }
            if (r.error() != Errors.NONE) throw new ClientException(r.error(), "commit");
            lastCommitted.putAll(toCommit);
        } catch (IOException e) { throw new ClientException("commit failed", e); }
    }

    // ---- group membership ----------------------------------------------------------------------------

    private void rejoin() {
        List<TopicPartition> old = assignment;
        if (!old.isEmpty()) {
            listener.onPartitionsRevoked(old);
            if (cfg.autoCommit()) { try { commitSync(); } catch (ClientException ignored) { } }
        }
        int size = BrokerConnection.strSize(cfg.groupId()) + BrokerConnection.strSize(memberId) + 8 + 4;
        for (String t : topics) size += BrokerConnection.strSize(t);
        ByteBuffer req = ByteBuffer.allocate(size);
        BrokerConnection.putStr(req, cfg.groupId());
        BrokerConnection.putStr(req, memberId);
        req.putLong(cfg.sessionTimeoutMs()).putInt(topics.size());
        topics.forEach(t -> BrokerConnection.putStr(req, t));
        req.flip();
        try {
            var r = conn.call(Api.JOIN_GROUP, req);
            if (r.error() != Errors.NONE) throw new ClientException(r.error(), "join");
            ByteBuffer b = r.body();
            memberId = BrokerConnection.getStr(b);
            generation = b.getInt();
            int n = b.getInt();
            List<TopicPartition> fresh = new ArrayList<>();
            for (int i = 0; i < n; i++) fresh.add(new TopicPartition(BrokerConnection.getStr(b), b.getInt()));
            Set<TopicPartition> keep = new HashSet<>(fresh);
            position.keySet().retainAll(keep);                      // retained partitions keep their local position (sticky win)
            lastCommitted.keySet().retainAll(keep);
            outstanding.keySet().retainAll(keep);
            delayed.removeIf(d -> !keep.contains(d.rec().tp()));
            for (TopicPartition tp : fresh) if (!position.containsKey(tp)) position.put(tp, initialPosition(tp));
            assignment = fresh;
            needsRejoin = false;
            lastAutoCommit = System.currentTimeMillis();
            listener.onPartitionsAssigned(fresh);
        } catch (IOException e) { throw new ClientException("join failed", e); }
    }

    private long initialPosition(TopicPartition tp) throws IOException {
        ByteBuffer req = ByteBuffer.allocate(BrokerConnection.strSize(cfg.groupId()) + BrokerConnection.strSize(tp.topic()) + 4);
        BrokerConnection.putStr(req, cfg.groupId());
        BrokerConnection.putStr(req, tp.topic());
        req.putInt(tp.partition()).flip();
        var r = conn.call(Api.FETCH_OFFSET, req);
        long committed = r.body().getLong();
        return committed >= 0 ? committed : (cfg.autoOffsetReset().equals("latest") ? Api.LATEST : Api.EARLIEST);
    }

    private void heartbeatLoop() {
        while (running) {
            sleep(cfg.heartbeatIntervalMs());
            String m = memberId;
            if (m.isEmpty() || !running) continue;
            ByteBuffer req = ByteBuffer.allocate(BrokerConnection.strSize(cfg.groupId()) + BrokerConnection.strSize(m) + 4);
            BrokerConnection.putStr(req, cfg.groupId());
            BrokerConnection.putStr(req, m);
            req.putInt(generation).flip();
            try {
                var r = hbConn.call(Api.HEARTBEAT, req);
                if (r.error() == Errors.REBALANCE_IN_PROGRESS) needsRejoin = true;
                else if (r.error() == Errors.UNKNOWN_MEMBER_ID) { memberId = ""; needsRejoin = true; }
            } catch (IOException ignored) { /* reconnects on the next beat */ }
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @Override public void close() {
        if (!running) return;
        running = false;
        heartbeat.interrupt();
        try {
            if (cfg.autoCommit()) commitSync();
        } catch (RuntimeException ignored) { }
        try {
            if (!memberId.isEmpty()) {
                ByteBuffer req = ByteBuffer.allocate(BrokerConnection.strSize(cfg.groupId()) + BrokerConnection.strSize(memberId));
                BrokerConnection.putStr(req, cfg.groupId());
                BrokerConnection.putStr(req, memberId);
                req.flip();
                conn.call(Api.LEAVE_GROUP, req);
            }
        } catch (IOException ignored) { }
        conn.close(); hbConn.close();
        if (dlqProducer != null) dlqProducer.close();
    }
}
