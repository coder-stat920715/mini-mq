package io.mq;

import io.mq.broker.Broker;
import io.mq.client.*;

import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Loopback smoke benchmark: java -cp ... io.mq.Benchmark [messages] [payloadBytes]. Not a rigorous benchmark. */
public class Benchmark {
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        int size = args.length > 1 ? Integer.parseInt(args[1]) : 256;
        try (Broker br = new Broker(Broker.Config.defaults(Files.createTempDirectory("bench"), 0)).start();
             Admin admin = new Admin("localhost", br.port())) {
            admin.createTopic("bench", 8);
            byte[] payload = new byte[size];
            long t0 = System.nanoTime();
            CompletableFuture<?> last = null;
            try (MqProducer p = new MqProducer(new MqProducer.Config("localhost", br.port(), io.mq.domain.Acks.ALL,
                    128 * 1024, 64L << 20, 2, 5, true, 30_000, ""))) {
                for (int i = 0; i < n; i++) last = p.send("bench", null, payload);
                p.flush();
            }
            double s = (System.nanoTime() - t0) / 1e9;
            System.out.printf("produce: %,d msgs in %.2fs = %,.0f msg/s (%.1f MB/s)%n", n, s, n / s, n * (double) size / s / 1e6);
            t0 = System.nanoTime();
            long got = 0;
            try (MqConsumer c = new MqConsumer(MqConsumer.Config.defaults("localhost", br.port(), "bench").withAutoCommit(false))) {
                c.subscribe(java.util.List.of("bench"), null);
                while (got < n) got += c.poll(Duration.ofMillis(100)).size();
            }
            s = (System.nanoTime() - t0) / 1e9;
            System.out.printf("consume: %,d msgs in %.2fs = %,.0f msg/s%n", got, s, got / s);
        }
    }
}
