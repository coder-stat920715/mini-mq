package io.mq;

import io.mq.broker.Broker;
import io.mq.client.*;
import io.mq.domain.Acks;
import io.mq.domain.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** Real broker on a random port, real clients over TCP. */
class EndToEndTest {

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private static Broker broker(Path dir) throws Exception {
        return new Broker(Broker.Config.defaults(dir, 0).withSegmentBytes(256 * 1024)).start();
    }

    @Test
    void producesAndConsumesWithPerKeyOrderAndNoLoss() throws Exception {
        Path dir = Files.createTempDirectory("mq");
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("orders", 4);
            int total = 5000;
            try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", br.port()))) {
                List<CompletableFuture<MqProducer.RecordMetadata>> fs = new ArrayList<>();
                for (int i = 0; i < total; i++) fs.add(p.send("orders", b("key-" + (i % 20)), b(Integer.toString(i))));
                p.flush();
                for (var f : fs) assertTrue(f.get().offset() >= 0);
            }
            Map<String, Integer> lastSeen = new HashMap<>();
            Set<Integer> seen = new HashSet<>();
            try (MqConsumer c = new MqConsumer(MqConsumer.Config.defaults("localhost", br.port(), "g1"))) {
                c.subscribe(List.of("orders"), null);
                long end = System.currentTimeMillis() + 15_000;
                while (seen.size() < total && System.currentTimeMillis() < end) {
                    for (ConsumerRecord r : c.poll(Duration.ofMillis(200))) {
                        int v = Integer.parseInt(r.valueAsString());
                        assertTrue(seen.add(v), "duplicate delivery of " + v);
                        Integer prev = lastSeen.put(r.keyAsString(), v);
                        assertTrue(prev == null || prev < v, "per-key order violated for " + r.keyAsString());
                    }
                }
            }
            assertEquals(total, seen.size());
        }
    }

    @Test
    void groupSplitsPartitionsAndResumesFromCommittedOffsets() throws Exception {
        Path dir = Files.createTempDirectory("mq");
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("events", 4);
            try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", br.port()))) {
                for (int i = 0; i < 400; i++) p.send("events", 0 + (i % 4), null, b("m" + i));
                p.flush();
            }
            var cfg = MqConsumer.Config.defaults("localhost", br.port(), "grp");
            try (MqConsumer c1 = new MqConsumer(cfg); MqConsumer c2 = new MqConsumer(cfg)) {
                c1.subscribe(List.of("events"), null);
                c2.subscribe(List.of("events"), null);
                c1.poll(Duration.ofMillis(50));
                c2.poll(Duration.ofMillis(50));
                Thread.sleep(1500);                               // heartbeats notice the generation bump
                c1.poll(Duration.ofMillis(50));
                c2.poll(Duration.ofMillis(50));
                Set<TopicPartition> all = new HashSet<>(c1.assignment());
                all.addAll(c2.assignment());
                assertEquals(4, all.size(), "every partition owned exactly once");
                assertEquals(2, c1.assignment().size());
                assertEquals(2, c2.assignment().size());
            }
            // everything consumed above was auto-committed on close; a new consumer sees only new data
            try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", br.port()))) {
                p.send("events", 0, null, b("fresh")).get();
            }
            try (MqConsumer c = new MqConsumer(cfg)) {
                c.subscribe(List.of("events"), null);
                List<String> got = new ArrayList<>();
                long end = System.currentTimeMillis() + 10_000;
                while (got.isEmpty() && System.currentTimeMillis() < end)
                    c.poll(Duration.ofMillis(200)).forEach(r -> got.add(r.valueAsString()));
                assertEquals(List.of("fresh"), got.stream().filter(s -> s.equals("fresh")).toList());
                assertTrue(got.size() < 400, "must not re-read the whole topic");
            }
        }
    }

    @Test
    void idempotentProducerDeduplicatesResentBatch() throws Exception {
        Path dir = Files.createTempDirectory("mq");
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("t", 1);
            var conn = new BrokerConnection("localhost", br.port());
            var pid = conn.call(io.mq.domain.Api.INIT_PRODUCER_ID, str(""));
            long id = pid.body().getLong();
            short epoch = pid.body().getShort();
            java.nio.ByteBuffer req = produceBatch("t", id, epoch, 0, "x");
            long first = conn.call(io.mq.domain.Api.PRODUCE, req.duplicate()).body().getLong();
            long again = conn.call(io.mq.domain.Api.PRODUCE, req.duplicate()).body().getLong();   // client retry
            assertEquals(first, again);
            assertEquals(1L, br.logs().log(new TopicPartition("t", 0)).logEndOffset(), "retry must not append twice");
            var gap = conn.call(io.mq.domain.Api.PRODUCE, produceBatch("t", id, epoch, 5, "y"));    // seq gap
            assertEquals(io.mq.domain.Errors.OUT_OF_ORDER_SEQUENCE, gap.error());
            // a new instance with the same transactional id fences the old epoch
            var t1 = conn.call(io.mq.domain.Api.INIT_PRODUCER_ID, str("txn-1"));
            var t2 = conn.call(io.mq.domain.Api.INIT_PRODUCER_ID, str("txn-1"));
            long tp = t1.body().getLong(); short e1 = t1.body().getShort();
            assertEquals(tp, t2.body().getLong());
            short e2 = t2.body().getShort();
            assertTrue(e2 > e1);
            assertEquals(io.mq.domain.Errors.PRODUCER_FENCED, conn.call(io.mq.domain.Api.PRODUCE, produceBatch("t", tp, e1, 0, "z")).error());
            conn.close();
        }
    }

    @Test
    void nackRetriesThenDeadLetters() throws Exception {
        Path dir = Files.createTempDirectory("mq");
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("jobs", 1);
            try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", br.port()))) {
                p.send("jobs", null, b("poison")).get();
                p.send("jobs", null, b("good")).get();
            }
            var cfg = new MqConsumer.Config("localhost", br.port(), "workers", true, 200, 10_000, 1000, 1 << 20, "earliest", 2, 20);
            int poisonDeliveries = 0;
            boolean goodSeen = false;
            try (MqConsumer c = new MqConsumer(cfg)) {
                c.subscribe(List.of("jobs"), null);
                long end = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < end && (poisonDeliveries < 3 || !goodSeen)) {
                    for (ConsumerRecord r : c.poll(Duration.ofMillis(100))) {
                        if (r.valueAsString().equals("poison")) { poisonDeliveries++; c.nack(r); }
                        else { goodSeen = true; c.ack(r); }
                    }
                }
            }
            assertEquals(3, poisonDeliveries, "1 delivery + 2 retries");
            assertTrue(goodSeen);
            try (MqConsumer dlq = new MqConsumer(MqConsumer.Config.defaults("localhost", br.port(), "dlq-reader"))) {
                dlq.subscribe(List.of("jobs.DLQ"), null);
                List<String> got = new ArrayList<>();
                long end = System.currentTimeMillis() + 5_000;
                while (got.isEmpty() && System.currentTimeMillis() < end) dlq.poll(Duration.ofMillis(100)).forEach(r -> got.add(r.valueAsString()));
                assertEquals(List.of("poison"), got);
            }
        }
    }

    @Test
    void dataSurvivesBrokerRestart() throws Exception {
        Path dir = Files.createTempDirectory("mq");
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("durable", 2);
            try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", br.port()))) {
                for (int i = 0; i < 100; i++) p.send("durable", i % 2, null, b("v" + i));
                p.flush();
            }
            try (MqConsumer c = new MqConsumer(MqConsumer.Config.defaults("localhost", br.port(), "r"))) {
                c.subscribe(List.of("durable"), null);
                int n = 0; long end = System.currentTimeMillis() + 10_000;
                while (n < 40 && System.currentTimeMillis() < end) n += c.poll(Duration.ofMillis(100)).size();
            }
        }
        try (Broker br = broker(dir); Admin admin = new Admin("localhost", br.port())) {
            assertEquals(2, admin.topics().get("durable").intValue());
            assertEquals(100L, br.logs().log(new TopicPartition("durable", 0)).logEndOffset()
                    + br.logs().log(new TopicPartition("durable", 1)).logEndOffset());
            try (MqConsumer c = new MqConsumer(MqConsumer.Config.defaults("localhost", br.port(), "fresh-group"))) {
                c.subscribe(List.of("durable"), null);
                Set<String> got = new HashSet<>();
                long end = System.currentTimeMillis() + 10_000;
                while (got.size() < 100 && System.currentTimeMillis() < end) c.poll(Duration.ofMillis(100)).forEach(r -> got.add(r.valueAsString()));
                assertEquals(100, got.size());
            }
        }
    }

    // ---- raw protocol helpers for the idempotence test ---------------------------------------------

    private static java.nio.ByteBuffer str(String s) {
        byte[] x = s.getBytes(StandardCharsets.UTF_8);
        return (java.nio.ByteBuffer) java.nio.ByteBuffer.allocate(2 + x.length).putShort((short) x.length).put(x).flip();
    }

    private static java.nio.ByteBuffer produceBatch(String topic, long pid, short epoch, int seq, String value) {
        byte[] t = topic.getBytes(StandardCharsets.UTF_8), v = value.getBytes(StandardCharsets.UTF_8);
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(2 + t.length + 4 + 8 + 2 + 4 + 1 + 4 + 8 + 4 + 4 + v.length);
        b.putShort((short) t.length).put(t).putInt(0).putLong(pid).putShort(epoch).putInt(seq).put(Acks.ALL.code).putInt(1);
        b.putLong(1L).putInt(-1).putInt(v.length).put(v);
        return (java.nio.ByteBuffer) b.flip();
    }
}
