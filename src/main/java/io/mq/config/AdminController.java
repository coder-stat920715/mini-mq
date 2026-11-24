package io.mq.config;

import io.mq.broker.Broker;
import io.mq.domain.TopicPartition;
import io.mq.storage.AppendOnlyLog;
import io.mq.storage.LogManager;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.*;

/** Reactive admin plane. Blocking disk work is moved off the event loop onto boundedElastic. */
@RestController
@RequestMapping("/admin")
public class AdminController {

    public record CreateTopic(String name, int partitions) {}
    public record PartitionInfo(int partition, long startOffset, long endOffset, long highWatermark, int segments, long sizeBytes) {}

    private final Broker broker;

    public AdminController(Broker broker) { this.broker = broker; }

    @GetMapping("/topics")
    public Mono<Map<String, Integer>> topics() {
        return Mono.fromCallable(() -> {
            Map<String, Integer> out = new TreeMap<>(broker.logs().topics());
            out.keySet().removeIf(t -> t.startsWith("__"));
            return out;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Void> create(@RequestBody CreateTopic req) {
        return Mono.<Void>fromCallable(() -> {
            try {
                broker.logs().createTopic(req.name(), req.partitions());
            } catch (LogManager.InvalidTopicException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
            } catch (LogManager.TopicExistsException e) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
            }
            return null;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/topics/{topic}")
    public Mono<List<PartitionInfo>> partitions(@PathVariable String topic) {
        return Mono.fromCallable(() -> {
            int n = broker.logs().partitions(topic);
            if (n < 0) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown topic");
            List<PartitionInfo> out = new ArrayList<>();
            for (int p = 0; p < n; p++) {
                AppendOnlyLog l = broker.logs().log(new TopicPartition(topic, p));
                out.add(new PartitionInfo(p, l.logStartOffset(), l.logEndOffset(), l.highWatermark(), l.segmentCount(), l.sizeBytes()));
            }
            return out;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/groups")
    public Map<String, Map<String, List<TopicPartition>>> groups() { return broker.groups().describe(); }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        var h = broker.handler();
        return Map.of("messagesIn", h.messagesIn.sum(), "bytesIn", h.bytesIn.sum(),
                "fetchRequests", h.fetchRequests.sum(), "bytesOut", h.bytesOut.sum());
    }
}
