package io.mq;

import io.mq.storage.AppendOnlyLog;
import io.mq.storage.AppendOnlyLog.Rec;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppendOnlyLogTest {

    private static Rec rec(String k, String v) {
        return new Rec(1L, k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
    }

    /** Decodes the raw entries of a slice the way a client does. */
    private static List<String> values(AppendOnlyLog.Slice s) throws Exception {
        List<String> out = new ArrayList<>();
        if (s.count() == 0) return out;
        try (FileChannel ch = FileChannel.open(s.file(), StandardOpenOption.READ)) {
            ByteBuffer b = ByteBuffer.allocate((int) s.length());
            ch.read(b, s.position());
            b.flip();
            while (b.hasRemaining()) {
                b.getLong(); b.getInt(); b.getInt(); b.getLong();
                int kl = b.getInt(); b.position(b.position() + Math.max(kl, 0));
                int vl = b.getInt(); byte[] v = new byte[vl]; b.get(v);
                out.add(new String(v, StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    @Test
    void appendAndReadBack() throws Exception {
        Path dir = Files.createTempDirectory("log");
        try (AppendOnlyLog log = new AppendOnlyLog(dir, 1 << 20, 64)) {
            assertEquals(0L, log.append(List.of(rec("a", "v0"), rec("a", "v1"), rec("b", "v2"))));
            assertEquals(3L, log.append(List.of(rec("c", "v3"))));
            assertEquals(4L, log.logEndOffset());
            var s = log.read(1, 1 << 20);
            assertEquals(List.of("v1", "v2", "v3"), values(s));
            assertEquals(4L, s.nextOffset());
            assertEquals(0, log.read(4, 1024).count());          // caught up
            assertEquals(4, log.read(-2, 1 << 20).count());      // earliest
            assertEquals(0, log.read(-1, 1 << 20).count());      // latest
        }
    }

    @Test
    void rollsSegmentsAndKeepsOrderAcrossThem() throws Exception {
        Path dir = Files.createTempDirectory("log");
        try (AppendOnlyLog log = new AppendOnlyLog(dir, 512, 64)) {
            for (int i = 0; i < 200; i++) log.append(List.of(rec("k", "value-" + i)));
            assertTrue(log.segmentCount() > 3, "expected several segments, got " + log.segmentCount());
            long next = 0;
            List<String> all = new ArrayList<>();
            while (next < log.logEndOffset()) {                 // slices never cross segments: keep fetching
                var s = log.read(next, 100_000);
                assertTrue(s.count() > 0);
                all.addAll(values(s));
                next = s.nextOffset();
            }
            assertEquals(200, all.size());
            for (int i = 0; i < 200; i++) assertEquals("value-" + i, all.get(i));
        }
    }

    @Test
    void recoversAndTruncatesTornWrite() throws Exception {
        Path dir = Files.createTempDirectory("log");
        try (AppendOnlyLog log = new AppendOnlyLog(dir, 1 << 20, 64)) {
            log.append(List.of(rec("a", "one"), rec("a", "two"), rec("a", "three")));
        }
        Path file = dir.resolve("00000000000000000000.log");
        try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "rw")) {
            f.setLength(f.length() - 3);                        // simulate a crash mid-write of the last entry
        }
        try (AppendOnlyLog log = new AppendOnlyLog(dir, 1 << 20, 64)) {
            assertEquals(2L, log.logEndOffset());
            assertEquals(List.of("one", "two"), values(log.read(0, 1 << 20)));
            assertEquals(2L, log.append(List.of(rec("a", "three-again"))));   // offsets continue cleanly
        }
    }

    @Test
    void sizeRetentionDeletesOldestClosedSegmentsOnly() throws Exception {
        Path dir = Files.createTempDirectory("log");
        try (AppendOnlyLog log = new AppendOnlyLog(dir, 512, 64)) {
            for (int i = 0; i < 100; i++) log.append(List.of(rec("k", "value-" + i)));
            int before = log.segmentCount();
            int deleted = log.enforceRetention(0, 1024);
            assertTrue(deleted > 0);
            assertEquals(before - deleted, log.segmentCount());
            assertTrue(log.logStartOffset() > 0);
            assertEquals(100L, log.logEndOffset());
            assertThrows(AppendOnlyLog.OffsetOutOfRangeException.class, () -> log.read(0, 1024));
        }
    }
}
