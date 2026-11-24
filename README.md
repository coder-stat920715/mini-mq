# mini-mq

A small Kafka-style distributed log broker in **Java 17 + Spring Boot 3 + Netty**: partitioned append-only
logs, zero-copy fetch, consumer groups with rebalancing, idempotent producers, ack/nack with retry and a
dead-letter topic. It is the working companion to the HLD/LLD document.

> **Status: single-node.** Replication/ISR, Raft metadata, and transactions are *designed* in the HLD
> but **not implemented** here (see "Not implemented"). Treat this as a solid, tested core to extend, not
> a drop-in production broker.

## Run

```bash
mvn spring-boot:run            # data plane on :9092, admin REST + actuator on :8080
# or
docker compose up --build
```

Admin API (WebFlux):

```bash
curl -X POST localhost:8080/admin/topics -H 'content-type: application/json' -d '{"name":"orders","partitions":4}'
curl localhost:8080/admin/topics
curl localhost:8080/admin/topics/orders     # per-partition start/end offset, HW, segments, size
curl localhost:8080/admin/groups
curl localhost:8080/admin/stats
```

## Client usage

```java
try (Admin admin = new Admin("localhost", 9092)) { admin.createTopic("orders", 4); }

try (MqProducer p = new MqProducer(MqProducer.Config.defaults("localhost", 9092))) {   // idempotent, acks=all
    p.send("orders", "customer-42".getBytes(), "{\"id\":1}".getBytes());                // same key -> same partition -> ordered
    p.flush();
}

var cfg = MqConsumer.Config.defaults("localhost", 9092, "billing").withAutoCommit(false);
try (MqConsumer c = new MqConsumer(cfg)) {
    c.subscribe(List.of("orders"), new RebalanceListener() {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> ps) { c.commitSync(); }
    });
    while (true) {
        for (ConsumerRecord r : c.poll(Duration.ofMillis(500))) {
            try { handle(r); c.ack(r); } catch (Exception e) { c.nack(r); }   // retry w/ backoff, then orders.DLQ
        }
        c.commitSync();                                                         // at-least-once
    }
}
```

Delivery semantics: `Acks.NONE` + commit-before-process = at-most-once; `Acks.ALL` + commit-after-process =
at-least-once; idempotent producer (default) removes producer-side duplicates.

## Layout

| Package | What |
|---|---|
| `io.mq.storage` | `LogSegment`, `AppendOnlyLog`, `LogManager`: segment files, sparse index, CRC32C, crash recovery, size/time retention |
| `io.mq.broker` | `RequestHandler` + `BrokerServer` (Netty), `GroupCoordinator`, `OffsetStore`, `ProducerStateManager`, `Broker` (wiring) |
| `io.mq.client` | `MqProducer` (batching + direct-buffer pool + idempotence), `MqConsumer`, `Admin`, `BrokerConnection` |
| `io.mq.config` | Spring Boot layer: `MqProperties`, `BrokerConfig`, WebFlux `AdminController` |
| `io.mq.domain` | `TopicPartition`, `Acks`, `Api`, `Errors` |

The broker core (`io.mq.storage|broker|client|domain`) has **no Spring dependency**; `new Broker(Config).start()` runs it
anywhere (the tests do exactly that).

## Design notes

* **On-disk format = wire format.** Entry: `offset:8 | size:4 | crc32c:4 | ts:8 | keyLen:4 | key | valLen:4 | value`.
  Fetch replies are a small header plus a Netty `DefaultFileRegion` over the segment file (`sendfile`, no heap copy).
* **Threading.** Netty event loops only frame/parse; each partition is pinned to one single-thread executor, so appends
  are uncontended and ordered. Long-poll fetches are parked with scheduled retries, never a blocked thread.
* **Idempotence.** Each batch carries `(producerId, epoch, baseSequence)`. A resend of the last batch returns the original
  offset without appending; gaps return `OUT_OF_ORDER_SEQUENCE`; a re-`init` with the same transactional id bumps the epoch and
  fences the old instance (`PRODUCER_FENCED`).
* **Groups.** Eager rebalance computed by the coordinator (RANGE / ROUND_ROBIN / STICKY). Heartbeat returns
  `REBALANCE_IN_PROGRESS`; members re-join to get the new plan. Commits with a stale generation are rejected (zombie fencing).
  Offsets live in the internal topic `__consumer_offsets` and are rebuilt by replay on startup.
* **Queue semantics.** `nack` redelivers with exponential backoff; after `maxRetries` the record goes to `<topic>.DLQ`.
  Commit never advances past a record still awaiting redelivery.

## Wire protocol

Request `[len:4][api:2][corr:4][body]`, response `[len:4][corr:4][err:2][body]`. APIs: CREATE_TOPIC, PRODUCE, FETCH, COMMIT_OFFSET,
FETCH_OFFSET, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, METADATA, INIT_PRODUCER_ID (see `io.mq.domain.Api` and `RequestHandler`).

## Verification status (be aware)

* The core (storage, protocol, broker, clients, coordinator) was compiled with `--release 17` and **13 tests pass** against a real
  broker over TCP: append/read/roll/recover/retention, group assignment and fencing, 5,000-message ordered consume, rebalance
  across two consumers, offset resume, idempotent dedup + epoch fencing, nack -> DLQ, restart recovery.
* Loopback smoke benchmark (1 vCPU sandbox, client and broker sharing the core, 256 B payload, acks=all): ~390k msg/s produce,
  ~530k msg/s consume. This says nothing about 500k msg/s on real multi-node hardware or p99 latency; benchmark your own setup.
* The Spring layer (`io.mq.config`, `pom.xml`, `SpringBootSmokeTest`) could **not be compiled in the build sandbox** (no Maven Central
  access), so run `mvn verify` once before relying on it. It is thin (properties, one bean, one controller).
* Tests were run in the sandbox with a minimal JUnit shim; under Maven they run on real JUnit 5.

## Not implemented (designed in the HLD)

Leader/follower replication, ISR and `min.insync.replicas > 1` (requests are rejected with `NOT_ENOUGH_REPLICAS` if configured),
Raft controller / multi-broker metadata, transaction coordinator and `read_committed` (only idempotence + epoch fencing exist),
log compaction, timestamp index, TLS/SASL/ACLs and quotas, compression, multi-broker client routing, delayed-retry topics on the
broker (retry timing is client-side).
