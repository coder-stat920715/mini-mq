package io.mq.broker;

import io.mq.domain.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Idempotent-producer state: (producerId, epoch) plus the last accepted sequence range per partition.
 * A resend of the last batch is answered with the original offset and NOT appended again.
 * Transactional ids map to a stable producerId whose epoch is bumped on every re-init, fencing zombies.
 * Callers must hold the partition log's monitor around check + append + recordAppend.
 */
public final class ProducerStateManager {

    public enum Verdict { APPEND, DUPLICATE, OUT_OF_ORDER, FENCED }
    public record Check(Verdict verdict, long duplicateOffset) {
        static final Check APPEND = new Check(Verdict.APPEND, -1);
    }
    public record ProducerId(long id, short epoch) {}
    private record Last(int baseSeq, int lastSeq, long baseOffset) {}

    private final java.util.concurrent.atomic.AtomicLong nextPid = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis());
    private final Map<String, Long> txnIdToPid = new ConcurrentHashMap<>();
    private final Map<Long, Short> epochs = new ConcurrentHashMap<>();
    private final Map<String, Last> last = new ConcurrentHashMap<>();

    public synchronized ProducerId init(String transactionalId) {
        if (transactionalId == null || transactionalId.isEmpty()) {
            long pid = nextPid.incrementAndGet();
            epochs.put(pid, (short) 0);
            return new ProducerId(pid, (short) 0);
        }
        long pid = txnIdToPid.computeIfAbsent(transactionalId, k -> nextPid.incrementAndGet());
        short epoch = (short) (epochs.getOrDefault(pid, (short) -1) + 1);   // bump: fences the previous instance
        epochs.put(pid, epoch);
        return new ProducerId(pid, epoch);
    }

    public Check check(long pid, short epoch, TopicPartition tp, int baseSeq) {
        Short known = epochs.get(pid);
        if (known == null) epochs.put(pid, epoch);              // pid minted before a broker restart
        else if (epoch < known) return new Check(Verdict.FENCED, -1);
        else if (epoch > known) epochs.put(pid, epoch);
        Last l = last.get(pid + "|" + tp);
        if (l == null || epoch > (known == null ? epoch : known)) return Check.APPEND;
        if (baseSeq == l.lastSeq() + 1) return Check.APPEND;
        if (baseSeq == l.baseSeq()) return new Check(Verdict.DUPLICATE, l.baseOffset());
        return new Check(Verdict.OUT_OF_ORDER, -1);
    }

    public void recordAppend(long pid, TopicPartition tp, int baseSeq, int count, long baseOffset) {
        last.put(pid + "|" + tp, new Last(baseSeq, baseSeq + count - 1, baseOffset));
    }
}
