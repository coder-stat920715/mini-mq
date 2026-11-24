package io.mq.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * Per-partition commit log: ordered, append-only, split into segments named by base offset.
 * Appends are serialized by the log's monitor (each partition is also pinned to one executor thread,
 * so the lock is uncontended). Reads are lock-free: they only touch bytes below the high watermark.
 */
public final class AppendOnlyLog implements Closeable {

    public record Rec(long timestamp, byte[] key, byte[] value) {}

    /** A byte range of the segment file made of whole entries; suitable for zero-copy transfer. */
    public record Slice(Path file, long position, long length, long firstOffset, long nextOffset, int count) {
        public static Slice empty(long next) { return new Slice(null, 0, 0, next, next, 0); }
    }

    public static final class OffsetOutOfRangeException extends RuntimeException {
        public OffsetOutOfRangeException(String m) { super(m); }
    }

    private final Path dir;
    private final long segmentBytes;
    private final int indexInterval;
    private final ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private volatile LogSegment active;
    private volatile long nextOffset;
    private volatile long highWatermark;

    public AppendOnlyLog(Path dir, long segmentBytes, int indexInterval) throws IOException {
        this.dir = Files.createDirectories(dir);
        this.segmentBytes = segmentBytes;
        this.indexInterval = indexInterval;
        recover();
    }

    private void recover() throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s.filter(f -> f.getFileName().toString().endsWith(".log")).sorted()::iterator) {
                long base = Long.parseLong(p.getFileName().toString().replace(".log", ""));
                segments.put(base, new LogSegment(dir, base, indexInterval));
            }
        }
        if (segments.isEmpty()) segments.put(0L, new LogSegment(dir, 0L, indexInterval));
        long next = 0;
        for (LogSegment seg : segments.values()) next = seg.recover();
        active = segments.lastEntry().getValue();
        nextOffset = next;
        highWatermark = next;   // single-node: everything recovered is committed
    }

    /** Assigns consecutive offsets and appends the batch. Returns the base offset. */
    public synchronized long append(List<Rec> recs) throws IOException {
        if (recs.isEmpty()) return nextOffset;
        long base = nextOffset;
        int batchBytes = 0;
        for (Rec r : recs) batchBytes += LogSegment.entryLength(r);
        if (active.size() > 0 && active.size() + batchBytes > segmentBytes) roll(base);
        active.appendBatch(base, recs);
        nextOffset = base + recs.size();
        highWatermark = nextOffset;   // replication would advance this to min(ISR LEO) instead
        return base;
    }

    private void roll(long base) throws IOException {
        active.flush();
        active = new LogSegment(dir, base, indexInterval);
        segments.put(base, active);
    }

    /**
     * Reads whole entries starting at {@code from}. -2 = earliest, -1 = latest.
     * Never crosses a segment boundary, so the result is one contiguous file range.
     */
    public Slice read(long from, int maxBytes) throws IOException {
        long hw = highWatermark;
        if (from == -2L) from = logStartOffset();
        else if (from == -1L) from = hw;
        if (from < logStartOffset() || from > nextOffset)
            throw new OffsetOutOfRangeException("offset " + from + " not in [" + logStartOffset() + "," + nextOffset + "]");
        if (from >= hw) return Slice.empty(from);
        Map.Entry<Long, LogSegment> e = segments.floorEntry(from);
        return e.getValue().read(from, hw, maxBytes);
    }

    public void forEach(BiConsumer<Long, Rec> visitor) throws IOException {
        for (LogSegment s : segments.values()) s.scan(visitor);
    }

    public long logStartOffset() { return segments.firstKey(); }
    public long logEndOffset() { return nextOffset; }
    public long highWatermark() { return highWatermark; }
    public int segmentCount() { return segments.size(); }
    public long sizeBytes() { return segments.values().stream().mapToLong(LogSegment::size).sum(); }

    public synchronized void flush() throws IOException { active.flush(); }

    /** Retention: drop whole closed segments older than cutoff or beyond the size cap (oldest first). */
    public synchronized int enforceRetention(long cutoffEpochMs, long maxBytes) throws IOException {
        int deleted = 0;
        long total = sizeBytes();
        for (var it = segments.entrySet().iterator(); it.hasNext(); ) {
            var e = it.next();
            if (e.getValue() == active) break;
            boolean old = cutoffEpochMs > 0 && e.getValue().lastModified() < cutoffEpochMs;
            boolean big = maxBytes > 0 && total > maxBytes;
            if (!old && !big) break;
            total -= e.getValue().size();
            e.getValue().delete();
            it.remove();
            deleted++;
        }
        return deleted;
    }

    @Override public synchronized void close() throws IOException {
        for (LogSegment s : segments.values()) s.close();
    }
}
