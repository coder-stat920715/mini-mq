package io.mq.storage;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.CRC32C;

/**
 * One append-only segment file. Entry layout (identical on disk and on the wire, which is what
 * makes zero-copy fetch possible):
 * <pre>
 * offset:int64 | size:int32 | crc32c:int32 | timestamp:int64 | keyLen:int32 | key | valLen:int32 | value
 *                 (size counts every byte after the size field; crc covers timestamp..value)
 * </pre>
 * A length of -1 means null (a null value is a tombstone). The sparse offset index is kept in memory
 * and rebuilt by scanning the segment at startup.
 */
final class LogSegment implements Closeable {
    static final int HEADER = 12;                 // offset + size
    private static final int MIN_BODY = 4 + 8 + 4 + 4;

    final long baseOffset;
    final Path path;
    private final FileChannel ch;
    private final int indexInterval;
    private final ConcurrentSkipListMap<Long, Integer> index = new ConcurrentSkipListMap<>();
    private volatile int size;                    // written only by the single appender
    private int sinceIndex;

    LogSegment(Path dir, long baseOffset, int indexInterval) throws IOException {
        this.baseOffset = baseOffset;
        this.indexInterval = indexInterval;
        this.path = dir.resolve(String.format("%020d.log", baseOffset));
        this.ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.size = (int) ch.size();
    }

    int size() { return size; }

    /** Validates entries, rebuilds the index, truncates a torn tail. Returns the next offset. */
    long recover() throws IOException {
        long fileSize = ch.size();
        int pos = 0;
        long next = baseOffset;
        ByteBuffer hdr = ByteBuffer.allocate(HEADER);
        index.clear();
        sinceIndex = 0;
        while (pos + HEADER <= fileSize) {
            hdr.clear(); readFully(hdr, pos); hdr.flip();
            long off = hdr.getLong();
            int sz = hdr.getInt();
            if (sz < MIN_BODY || pos + HEADER + (long) sz > fileSize) break;
            ByteBuffer body = ByteBuffer.allocate(sz);
            readFully(body, pos + HEADER); body.flip();
            int crc = body.getInt();
            CRC32C c = new CRC32C();
            c.update(body.duplicate().position(4));
            if ((int) c.getValue() != crc) break;
            maybeIndex(off, pos, HEADER + sz);
            pos += HEADER + sz;
            next = off + 1;
        }
        if (pos < fileSize) ch.truncate(pos);
        size = pos;
        return next;
    }

    /** Encodes and writes a whole batch with one write call. Returns bytes written. */
    int appendBatch(long firstOffset, java.util.List<AppendOnlyLog.Rec> recs) throws IOException {
        int total = 0;
        for (var r : recs) total += entryLength(r);
        ByteBuffer b = ByteBuffer.allocate(total);
        long off = firstOffset;
        int pos = size;
        for (var r : recs) {
            int start = b.position();
            byte[] k = r.key(), v = r.value();
            int kl = k == null ? -1 : k.length, vl = v == null ? -1 : v.length;
            b.putLong(off).putInt(entryLength(r) - HEADER).putInt(0).putLong(r.timestamp());
            b.putInt(kl); if (k != null) b.put(k);
            b.putInt(vl); if (v != null) b.put(v);
            CRC32C c = new CRC32C();
            c.update(b.duplicate().position(start + HEADER + 4).limit(b.position()));
            b.putInt(start + HEADER, (int) c.getValue());
            maybeIndex(off, pos + start, b.position() - start);
            off++;
        }
        b.flip();
        long wpos = size;
        while (b.hasRemaining()) wpos += ch.write(b, wpos);
        size = (int) wpos;
        return total;
    }

    static int entryLength(AppendOnlyLog.Rec r) {
        return HEADER + 4 + 8 + 4 + (r.key() == null ? 0 : r.key().length) + 4 + (r.value() == null ? 0 : r.value().length);
    }

    private void maybeIndex(long offset, int pos, int len) {
        if (index.isEmpty() || sinceIndex >= indexInterval) { index.put(offset, pos); sinceIndex = 0; }
        sinceIndex += len;
    }

    /** Raw byte range of whole entries starting at the first entry with offset >= from (and < hw). */
    AppendOnlyLog.Slice read(long from, long hw, int maxBytes) throws IOException {
        int limit = size;
        Map.Entry<Long, Integer> e = index.floorEntry(from);
        int pos = e == null ? 0 : e.getValue();
        ByteBuffer hdr = ByteBuffer.allocate(HEADER);
        int start = -1;
        long first = -1;
        while (pos + HEADER <= limit) {
            hdr.clear(); readFully(hdr, pos); hdr.flip();
            long off = hdr.getLong(); int sz = hdr.getInt();
            if (off >= hw) return AppendOnlyLog.Slice.empty(from);
            if (off >= from) { start = pos; first = off; break; }
            pos += HEADER + sz;
        }
        if (start < 0) return AppendOnlyLog.Slice.empty(from);
        int end = start; long last = first; int count = 0;
        while (end + HEADER <= limit) {
            hdr.clear(); readFully(hdr, end); hdr.flip();
            long off = hdr.getLong(); int len = HEADER + hdr.getInt();
            if (off >= hw || end + len > limit) break;
            if (count > 0 && (end + len - start) > maxBytes) break;
            end += len; last = off; count++;
        }
        return new AppendOnlyLog.Slice(path, start, end - start, first, last + 1, count);
    }

    /** Decodes every entry (used to replay internal topics). */
    void scan(java.util.function.BiConsumer<Long, AppendOnlyLog.Rec> visitor) throws IOException {
        int pos = 0, limit = size;
        ByteBuffer hdr = ByteBuffer.allocate(HEADER);
        while (pos + HEADER <= limit) {
            hdr.clear(); readFully(hdr, pos); hdr.flip();
            long off = hdr.getLong(); int sz = hdr.getInt();
            ByteBuffer body = ByteBuffer.allocate(sz);
            readFully(body, pos + HEADER); body.flip();
            body.getInt();
            long ts = body.getLong();
            int kl = body.getInt(); byte[] k = null;
            if (kl >= 0) { k = new byte[kl]; body.get(k); }
            int vl = body.getInt(); byte[] v = null;
            if (vl >= 0) { v = new byte[vl]; body.get(v); }
            visitor.accept(off, new AppendOnlyLog.Rec(ts, k, v));
            pos += HEADER + sz;
        }
    }

    long lastModified() { return path.toFile().lastModified(); }
    void flush() throws IOException { ch.force(false); }
    void delete() throws IOException { ch.close(); java.nio.file.Files.deleteIfExists(path); }
    @Override public void close() throws IOException { if (ch.isOpen()) { ch.force(false); ch.close(); } }

    private void readFully(ByteBuffer buf, long pos) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) throw new EOFException();
            pos += n;
        }
    }
}
