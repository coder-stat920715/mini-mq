package io.mq.storage;

import io.mq.domain.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Owns every partition log, topic metadata (discovered from the data dir), flush and retention. */
public final class LogManager implements Closeable {

    public record Settings(Path dataDir, long segmentBytes, int indexIntervalBytes,
                           long retentionMs, long retentionBytes, long flushIntervalMs) {}

    public static final class TopicExistsException extends RuntimeException {
        public TopicExistsException(String t) { super("topic exists with a different partition count: " + t); }
    }
    public static final class InvalidTopicException extends RuntimeException {
        public InvalidTopicException(String t) { super("invalid topic name or partition count: " + t); }
    }

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    private final Settings settings;
    private final Map<String, Integer> topics = new ConcurrentHashMap<>();
    private final Map<TopicPartition, AppendOnlyLog> logs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "log-maintenance"); t.setDaemon(true); return t; });

    public LogManager(Settings settings) throws IOException {
        this.settings = settings;
        Files.createDirectories(settings.dataDir());
        try (Stream<Path> s = Files.list(settings.dataDir())) {
            for (Path p : (Iterable<Path>) s.filter(Files::isDirectory)::iterator) {
                String name = p.getFileName().toString();
                int i = name.lastIndexOf('-');
                if (i <= 0) continue;
                try {
                    String topic = name.substring(0, i);
                    int part = Integer.parseInt(name.substring(i + 1));
                    logs.put(new TopicPartition(topic, part), open(p));
                    topics.merge(topic, part + 1, Math::max);
                } catch (NumberFormatException ignored) { /* not a partition dir */ }
            }
        }
        scheduler.scheduleWithFixedDelay(this::maintain, settings.flushIntervalMs(), settings.flushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private AppendOnlyLog open(Path dir) throws IOException {
        return new AppendOnlyLog(dir, settings.segmentBytes(), settings.indexIntervalBytes());
    }

    public synchronized void createTopic(String topic, int partitions) throws IOException {
        if (!VALID.matcher(topic).matches() || partitions < 1 || partitions > 10_000) throw new InvalidTopicException(topic);
        Integer existing = topics.get(topic);
        if (existing != null) {
            if (existing == partitions) return;            // idempotent
            throw new TopicExistsException(topic);
        }
        for (int p = 0; p < partitions; p++)
            logs.put(new TopicPartition(topic, p), open(settings.dataDir().resolve(topic + "-" + p)));
        topics.put(topic, partitions);
    }

    public AppendOnlyLog log(TopicPartition tp) { return logs.get(tp); }
    public int partitions(String topic) { return topics.getOrDefault(topic, -1); }
    public Map<String, Integer> topics() { return new TreeMap<>(topics); }

    private void maintain() {
        long cutoff = settings.retentionMs() > 0 ? System.currentTimeMillis() - settings.retentionMs() : 0;
        for (var e : logs.entrySet()) {
            try {
                e.getValue().flush();
                if (!e.getKey().topic().startsWith("__"))      // never expire internal topics
                    e.getValue().enforceRetention(cutoff, settings.retentionBytes());
            } catch (IOException | RuntimeException ex) {
                // never let the scheduled task die: log and keep maintaining other partitions
                System.err.println("log maintenance failed for " + e.getKey() + ": " + ex);
            }
        }
    }

    @Override public void close() throws IOException {
        scheduler.shutdownNow();
        for (AppendOnlyLog l : logs.values()) l.close();
    }
}
