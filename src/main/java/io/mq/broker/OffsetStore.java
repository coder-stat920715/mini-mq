package io.mq.broker;

import io.mq.domain.TopicPartition;
import io.mq.storage.AppendOnlyLog;
import io.mq.storage.AppendOnlyLog.Rec;
import io.mq.storage.LogManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Committed offsets, persisted in the internal log topic {@code __consumer_offsets} (key = group, topic,
 * partition; value = offset) and rebuilt by replaying it at startup, so the latest commit per key wins.
 */
public final class OffsetStore {
    public static final String TOPIC = "__consumer_offsets";

    private final AppendOnlyLog log;
    private final Map<String, Long> committed = new ConcurrentHashMap<>();

    public OffsetStore(LogManager logs) throws IOException {
        logs.createTopic(TOPIC, 1);
        this.log = logs.log(new TopicPartition(TOPIC, 0));
        log.forEach((off, rec) -> {
            if (rec.value() == null) committed.remove(new String(rec.key(), StandardCharsets.UTF_8));
            else committed.put(new String(rec.key(), StandardCharsets.UTF_8), ByteBuffer.wrap(rec.value()).getLong());
        });
    }

    public void commit(String group, Map<TopicPartition, Long> offsets) throws IOException {
        List<Rec> recs = new ArrayList<>();
        long now = System.currentTimeMillis();
        offsets.forEach((tp, off) -> recs.add(new Rec(now, key(group, tp).getBytes(StandardCharsets.UTF_8),
                ByteBuffer.allocate(8).putLong(off).array())));
        log.append(recs);
        offsets.forEach((tp, off) -> committed.put(key(group, tp), off));
    }

    /** Returns -1 when nothing has been committed. */
    public long fetch(String group, TopicPartition tp) { return committed.getOrDefault(key(group, tp), -1L); }

    private static String key(String group, TopicPartition tp) { return group + '\0' + tp.topic() + '\0' + tp.partition(); }
}
