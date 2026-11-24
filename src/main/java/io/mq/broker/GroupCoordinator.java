package io.mq.broker;

import io.mq.domain.Errors;
import io.mq.domain.TopicPartition;

import java.util.*;
import java.util.function.ToIntFunction;

/**
 * Consumer-group membership and partition assignment (eager rebalance, coordinator-computed plan).
 * Any membership/subscription change bumps the generation and recomputes the assignment; members learn
 * about it through heartbeat (REBALANCE_IN_PROGRESS) and re-join to pick up their new slice.
 * Commits carrying a stale generation are rejected, which fences zombie consumers.
 */
public final class GroupCoordinator {

    public enum Strategy { RANGE, ROUND_ROBIN, STICKY }

    public record JoinResult(short error, String memberId, int generation, List<TopicPartition> assignment) {}

    private static final class Member {
        final String id; Set<String> topics; long deadlineMs; long sessionMs;
        List<TopicPartition> assignment = List.of();
        Member(String id) { this.id = id; }
    }
    private static final class Group {
        int generation;
        final Map<String, Member> members = new TreeMap<>();
    }

    private final Map<String, Group> groups = new HashMap<>();
    private final ToIntFunction<String> partitionCount;
    private final Strategy strategy;
    private final java.util.function.LongSupplier clock;

    public GroupCoordinator(ToIntFunction<String> partitionCount, Strategy strategy) {
        this(partitionCount, strategy, System::currentTimeMillis);
    }
    public GroupCoordinator(ToIntFunction<String> partitionCount, Strategy strategy, java.util.function.LongSupplier clock) {
        this.partitionCount = partitionCount; this.strategy = strategy; this.clock = clock;
    }

    public synchronized JoinResult join(String groupId, String memberId, Collection<String> topics, long sessionMs) {
        Group g = groups.computeIfAbsent(groupId, k -> new Group());
        boolean changed = false;
        Member m = (memberId == null || memberId.isEmpty()) ? null : g.members.get(memberId);
        Set<String> subscription = new TreeSet<>(topics);
        if (m == null) {
            m = new Member(memberId == null || memberId.isEmpty() ? UUID.randomUUID().toString() : memberId);
            g.members.put(m.id, m);
            changed = true;
        } else if (!m.topics.equals(subscription)) {
            changed = true;
        }
        m.topics = subscription;
        m.sessionMs = sessionMs;
        m.deadlineMs = clock.getAsLong() + sessionMs;
        if (changed) rebalance(g);
        return new JoinResult(Errors.NONE, m.id, g.generation, m.assignment);
    }

    public synchronized short heartbeat(String groupId, String memberId, int generation) {
        Group g = groups.get(groupId);
        Member m = g == null ? null : g.members.get(memberId);
        if (m == null) return Errors.UNKNOWN_MEMBER_ID;
        m.deadlineMs = clock.getAsLong() + m.sessionMs;
        return generation == g.generation ? Errors.NONE : Errors.REBALANCE_IN_PROGRESS;
    }

    public synchronized void leave(String groupId, String memberId) {
        Group g = groups.get(groupId);
        if (g != null && g.members.remove(memberId) != null) rebalance(g);
    }

    /** Offsets may be committed with generation < 0 by standalone (non-group) consumers. */
    public synchronized short validateCommit(String groupId, String memberId, int generation) {
        if (generation < 0) return Errors.NONE;
        Group g = groups.get(groupId);
        if (g == null || !g.members.containsKey(memberId)) return Errors.UNKNOWN_MEMBER_ID;
        return generation == g.generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
    }

    /** Called periodically: evict members whose session expired. */
    public synchronized int expireSessions() {
        int removed = 0;
        long now = clock.getAsLong();
        for (Group g : groups.values()) {
            if (g.members.values().removeIf(m -> m.deadlineMs < now)) { rebalance(g); removed++; }
        }
        return removed;
    }

    public synchronized Map<String, Map<String, List<TopicPartition>>> describe() {
        Map<String, Map<String, List<TopicPartition>>> out = new TreeMap<>();
        groups.forEach((id, g) -> {
            Map<String, List<TopicPartition>> m = new TreeMap<>();
            g.members.forEach((mid, mem) -> m.put(mid, mem.assignment));
            out.put(id + "@gen" + g.generation, m);
        });
        return out;
    }

    // ---- assignment ------------------------------------------------------------------------------

    private void rebalance(Group g) {
        g.generation++;
        Map<String, List<TopicPartition>> previous = new HashMap<>();
        g.members.forEach((id, m) -> previous.put(id, m.assignment));
        Map<String, List<TopicPartition>> plan = assign(g, previous);
        g.members.forEach((id, m) -> m.assignment = plan.getOrDefault(id, List.of()));
    }

    private Map<String, List<TopicPartition>> assign(Group g, Map<String, List<TopicPartition>> previous) {
        Map<String, List<TopicPartition>> plan = new HashMap<>();
        g.members.keySet().forEach(id -> plan.put(id, new ArrayList<>()));
        TreeSet<String> allTopics = new TreeSet<>();
        g.members.values().forEach(m -> allTopics.addAll(m.topics));

        switch (strategy) {
            case RANGE -> {
                for (String t : allTopics) {
                    int n = partitionCount.applyAsInt(t);
                    List<String> subs = subscribers(g, t);
                    if (n <= 0 || subs.isEmpty()) continue;
                    int per = n / subs.size(), extra = n % subs.size(), p = 0;
                    for (int i = 0; i < subs.size(); i++) {
                        int take = per + (i < extra ? 1 : 0);
                        for (int j = 0; j < take; j++) plan.get(subs.get(i)).add(new TopicPartition(t, p++));
                    }
                }
            }
            case ROUND_ROBIN -> {
                int rr = 0;
                for (TopicPartition tp : allPartitions(allTopics)) {
                    List<String> subs = subscribers(g, tp.topic());
                    if (subs.isEmpty()) continue;
                    plan.get(subs.get(rr++ % subs.size())).add(tp);
                }
            }
            case STICKY -> {
                List<TopicPartition> all = allPartitions(allTopics);
                int members = g.members.size();
                int cap = members == 0 ? 0 : (all.size() + members - 1) / members;
                Set<TopicPartition> unassigned = new TreeSet<>(all);
                for (var e : previous.entrySet()) {                      // keep what members already own
                    Member m = g.members.get(e.getKey());
                    for (TopicPartition tp : e.getValue()) {
                        if (plan.get(e.getKey()).size() < cap && unassigned.contains(tp) && m.topics.contains(tp.topic())) {
                            plan.get(e.getKey()).add(tp);
                            unassigned.remove(tp);
                        }
                    }
                }
                for (TopicPartition tp : unassigned) {                   // hand out the rest to the lightest member
                    subscribers(g, tp.topic()).stream()
                            .min(Comparator.comparingInt((String id) -> plan.get(id).size()).thenComparing(id -> id))
                            .ifPresent(id -> plan.get(id).add(tp));
                }
            }
        }
        plan.values().forEach(Collections::sort);
        return plan;
    }

    private List<String> subscribers(Group g, String topic) {
        List<String> subs = new ArrayList<>();
        g.members.forEach((id, m) -> { if (m.topics.contains(topic)) subs.add(id); });
        return subs;
    }

    private List<TopicPartition> allPartitions(Collection<String> topics) {
        List<TopicPartition> all = new ArrayList<>();
        for (String t : topics) for (int p = 0, n = partitionCount.applyAsInt(t); p < n; p++) all.add(new TopicPartition(t, p));
        return all;
    }
}
