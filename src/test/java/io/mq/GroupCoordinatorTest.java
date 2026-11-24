package io.mq;

import io.mq.broker.GroupCoordinator;
import io.mq.broker.GroupCoordinator.Strategy;
import io.mq.domain.Errors;
import io.mq.domain.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class GroupCoordinatorTest {

    @Test
    void rangeSplitsContiguously() {
        var gc = new GroupCoordinator(t -> 6, Strategy.RANGE);
        var a = gc.join("g", "a", List.of("t"), 10_000);
        var b = gc.join("g", "b", List.of("t"), 10_000);
        var a2 = gc.join("g", "a", List.of("t"), 10_000);        // re-join to pick up the new plan
        assertEquals(2, b.generation());
        assertEquals(List.of(new TopicPartition("t", 0), new TopicPartition("t", 1), new TopicPartition("t", 2)), a2.assignment());
        assertEquals(List.of(new TopicPartition("t", 3), new TopicPartition("t", 4), new TopicPartition("t", 5)), b.assignment());
    }

    @Test
    void stickyKeepsExistingOwnership() {
        var gc = new GroupCoordinator(t -> 6, Strategy.STICKY);
        gc.join("g", "a", List.of("t"), 10_000);
        var a1 = gc.join("g", "a", List.of("t"), 10_000);
        assertEquals(6, a1.assignment().size());
        gc.join("g", "b", List.of("t"), 10_000);
        var a2 = gc.join("g", "a", List.of("t"), 10_000);
        var b = gc.join("g", "b", List.of("t"), 10_000);
        assertEquals(3, a2.assignment().size());
        assertEquals(3, b.assignment().size());
        assertTrue(a1.assignment().containsAll(a2.assignment()), "a must keep a subset of what it owned");
    }

    @Test
    void heartbeatSignalsRebalanceAndExpiryEvictsDeadMembers() {
        AtomicLong now = new AtomicLong(0);
        var gc = new GroupCoordinator(t -> 4, Strategy.ROUND_ROBIN, now::get);
        var a = gc.join("g", "a", List.of("t"), 5_000);
        gc.join("g", "b", List.of("t"), 5_000);
        assertEquals(Errors.REBALANCE_IN_PROGRESS, gc.heartbeat("g", "a", a.generation()));   // someone joined
        now.set(6_000);
        gc.heartbeat("g", "a", 2);                                                           // a stays alive, b is silent
        now.set(11_000);
        assertEquals(1, gc.expireSessions());
        var a2 = gc.join("g", "a", List.of("t"), 5_000);
        assertEquals(4, a2.assignment().size());
    }

    @Test
    void staleGenerationCommitIsFenced() {
        var gc = new GroupCoordinator(t -> 2, Strategy.RANGE);
        var a = gc.join("g", "a", List.of("t"), 10_000);
        assertEquals(Errors.NONE, gc.validateCommit("g", "a", a.generation()));
        gc.join("g", "b", List.of("t"), 10_000);
        assertEquals(Errors.ILLEGAL_GENERATION, gc.validateCommit("g", "a", a.generation()));
        assertEquals(Errors.UNKNOWN_MEMBER_ID, gc.validateCommit("g", "ghost", 5));
    }
}
