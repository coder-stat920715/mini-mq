package io.mq.broker;

import io.mq.domain.Acks;
import io.mq.domain.Api;
import io.mq.domain.Errors;
import io.mq.domain.TopicPartition;
import io.mq.storage.AppendOnlyLog;
import io.mq.storage.AppendOnlyLog.Rec;
import io.mq.storage.LogManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Decodes one request frame, runs it, and writes the response. Frames are parsed on the Netty event loop
 * (cheap, no I/O); anything that touches disk runs on the partition's single-thread executor. Fetch is
 * answered with a header plus a {@link DefaultFileRegion}, i.e. sendfile straight from the page cache.
 *
 * Response frame: [len:int32][correlationId:int32][error:int16][body...]
 */
public final class RequestHandler {

    private static final class BadRequest extends RuntimeException {
        BadRequest(String m) { super(m); }
    }
    private record ProduceResult(short error, long baseOffset) {}

    public final LongAdder messagesIn = new LongAdder(), bytesIn = new LongAdder(),
            fetchRequests = new LongAdder(), bytesOut = new LongAdder();

    private final LogManager logs;
    private final GroupCoordinator groups;
    private final OffsetStore offsets;
    private final ProducerStateManager producers;
    private final PartitionExecutors executors;
    private final BrokerSettings settings;

    public RequestHandler(LogManager logs, GroupCoordinator groups, OffsetStore offsets,
                          ProducerStateManager producers, PartitionExecutors executors, BrokerSettings settings) {
        this.logs = logs; this.groups = groups; this.offsets = offsets;
        this.producers = producers; this.executors = executors; this.settings = settings;
    }

    public void handle(ChannelHandlerContext ctx, ByteBuf in) {
        int corr = -1;
        try {
            short api = in.readShort();
            corr = in.readInt();
            switch (api) {
                case Api.CREATE_TOPIC -> createTopic(ctx, corr, in);
                case Api.PRODUCE -> produce(ctx, corr, in);
                case Api.FETCH -> fetch(ctx, corr, in);
                case Api.COMMIT_OFFSET -> commit(ctx, corr, in);
                case Api.FETCH_OFFSET -> fetchOffset(ctx, corr, in);
                case Api.JOIN_GROUP -> join(ctx, corr, in);
                case Api.HEARTBEAT -> heartbeat(ctx, corr, in);
                case Api.LEAVE_GROUP -> leave(ctx, corr, in);
                case Api.METADATA -> metadata(ctx, corr, in);
                case Api.INIT_PRODUCER_ID -> initProducerId(ctx, corr, in);
                default -> reply(ctx, corr, Errors.INVALID_REQUEST, b -> {});
            }
        } catch (IndexOutOfBoundsException | BadRequest e) {
            reply(ctx, corr, Errors.INVALID_REQUEST, b -> {});
        }
    }

    // ---- admin / metadata ---------------------------------------------------------------------------

    private void createTopic(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        String topic = str(in);
        int partitions = in.readInt();
        executors.forKey("admin").execute(() -> {
            short err = Errors.NONE;
            try { logs.createTopic(topic, partitions); }
            catch (LogManager.TopicExistsException e) { err = Errors.TOPIC_EXISTS; }
            catch (LogManager.InvalidTopicException e) { err = Errors.INVALID_TOPIC; }
            catch (IOException e) { err = Errors.UNKNOWN_SERVER_ERROR; }
            reply(ctx, corr, err, b -> {});
        });
    }

    private void metadata(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        int n = in.readInt();
        if (n < 0 || n > 10_000) throw new BadRequest("topics");
        Map<String, Integer> all = logs.topics();
        Map<String, Integer> out = new TreeMap<>();
        if (n == 0) all.forEach((t, p) -> { if (!t.startsWith("__")) out.put(t, p); });
        else for (int i = 0; i < n; i++) { String t = str(in); if (all.containsKey(t)) out.put(t, all.get(t)); }
        reply(ctx, corr, Errors.NONE, b -> {
            b.writeInt(out.size());
            out.forEach((t, p) -> { writeStr(b, t); b.writeInt(p); });
        });
    }

    private void initProducerId(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        String txnId = str(in);
        var pid = producers.init(txnId);
        reply(ctx, corr, Errors.NONE, b -> b.writeLong(pid.id()).writeShort(pid.epoch()));
    }

    // ---- produce -------------------------------------------------------------------------------------

    private void produce(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        TopicPartition tp = new TopicPartition(str(in), in.readInt());
        long pid = in.readLong();
        short epoch = in.readShort();
        int baseSeq = in.readInt();
        Acks acks = Acks.of(in.readByte());
        int count = in.readInt();
        if (count < 0 || count > in.readableBytes() / 16) throw new BadRequest("count");
        List<Rec> recs = new ArrayList<>(count);
        long bytes = 0;
        for (int i = 0; i < count; i++) {
            long ts = in.readLong();
            byte[] k = bytes(in), v = bytes(in);
            recs.add(new Rec(ts, k, v));
            bytes += (k == null ? 0 : k.length) + (v == null ? 0 : v.length);
        }
        long payload = bytes;
        executors.forPartition(tp).execute(() -> {
            ProduceResult r = doProduce(tp, pid, epoch, baseSeq, acks, recs);
            if (r.error() == Errors.NONE) { messagesIn.add(recs.size()); bytesIn.add(payload); }
            if (acks != Acks.NONE) reply(ctx, corr, r.error(), b -> b.writeLong(r.baseOffset()));
        });
    }

    private ProduceResult doProduce(TopicPartition tp, long pid, short epoch, int baseSeq, Acks acks, List<Rec> recs) {
        AppendOnlyLog log = logs.log(tp);
        if (log == null) return new ProduceResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1);
        if (acks == Acks.ALL && settings.minInsyncReplicas() > 1) return new ProduceResult(Errors.NOT_ENOUGH_REPLICAS, -1);
        try {
            synchronized (log) {   // check + append + record must be atomic per partition
                if (pid >= 0) {
                    var check = producers.check(pid, epoch, tp, baseSeq);
                    switch (check.verdict()) {
                        case FENCED -> { return new ProduceResult(Errors.PRODUCER_FENCED, -1); }
                        case OUT_OF_ORDER -> { return new ProduceResult(Errors.OUT_OF_ORDER_SEQUENCE, -1); }
                        case DUPLICATE -> { return new ProduceResult(Errors.NONE, check.duplicateOffset()); }
                        default -> { }
                    }
                }
                long base = log.append(recs);
                if (pid >= 0) producers.recordAppend(pid, tp, baseSeq, recs.size(), base);
                return new ProduceResult(Errors.NONE, base);
            }
        } catch (IOException e) {
            return new ProduceResult(Errors.UNKNOWN_SERVER_ERROR, -1);
        }
    }

    // ---- fetch (long poll + zero-copy) -----------------------------------------------------------

    private void fetch(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        TopicPartition tp = new TopicPartition(str(in), in.readInt());
        long offset = in.readLong();
        int maxBytes = Math.min(Math.max(in.readInt(), 1), settings.maxFetchBytes());
        int maxWaitMs = Math.min(Math.max(in.readInt(), 0), 30_000);
        fetchRequests.increment();
        runFetch(ctx, corr, tp, offset, maxBytes, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs));
    }

    private void runFetch(ChannelHandlerContext ctx, int corr, TopicPartition tp, long offset, int maxBytes, long deadlineNanos) {
        executors.forPartition(tp).execute(() -> {
            AppendOnlyLog log = logs.log(tp);
            if (log == null) { reply(ctx, corr, Errors.UNKNOWN_TOPIC_OR_PARTITION, b -> {}); return; }
            try {
                AppendOnlyLog.Slice s = log.read(offset, maxBytes);
                if (s.count() == 0 && System.nanoTime() < deadlineNanos && ctx.channel().isActive()) {
                    // nothing yet: park the request without blocking a thread (poll every 2 ms until the deadline)
                    ctx.executor().schedule(() -> runFetch(ctx, corr, tp, offset, maxBytes, deadlineNanos), 2, TimeUnit.MILLISECONDS);
                    return;
                }
                replyFetch(ctx, corr, log.highWatermark(), s);
            } catch (AppendOnlyLog.OffsetOutOfRangeException e) {
                reply(ctx, corr, Errors.OFFSET_OUT_OF_RANGE, b -> {});
            } catch (IOException e) {
                reply(ctx, corr, Errors.UNKNOWN_SERVER_ERROR, b -> {});
            }
        });
    }

    private void replyFetch(ChannelHandlerContext ctx, int corr, long hw, AppendOnlyLog.Slice s) {
        int dataLen = (int) s.length();
        ByteBuf h = ctx.alloc().buffer(30);
        h.writeInt(4 + 2 + 8 + 8 + 4 + dataLen).writeInt(corr).writeShort(Errors.NONE)
                .writeLong(hw).writeLong(s.nextOffset()).writeInt(dataLen);
        ctx.write(h);
        if (dataLen > 0) {
            ctx.write(new DefaultFileRegion(s.file().toFile(), s.position(), s.length()));   // zero-copy
            bytesOut.add(dataLen);
        }
        ctx.flush();
    }

    // ---- offsets and groups ------------------------------------------------------------------------

    private void commit(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        String group = str(in);
        int generation = in.readInt();
        String member = str(in);
        int n = in.readInt();
        if (n < 0 || n > 100_000) throw new BadRequest("count");
        Map<TopicPartition, Long> m = new HashMap<>();
        for (int i = 0; i < n; i++) m.put(new TopicPartition(str(in), in.readInt()), in.readLong());
        executors.forKey(group).execute(() -> {
            short err = groups.validateCommit(group, member, generation);
            if (err == Errors.NONE) {
                try { offsets.commit(group, m); } catch (IOException e) { err = Errors.UNKNOWN_SERVER_ERROR; }
            }
            short result = err;
            reply(ctx, corr, result, b -> {});
        });
    }

    private void fetchOffset(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        String group = str(in);
        TopicPartition tp = new TopicPartition(str(in), in.readInt());
        long off = offsets.fetch(group, tp);
        reply(ctx, corr, Errors.NONE, b -> b.writeLong(off));
    }

    private void join(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        String group = str(in), member = str(in);
        long sessionMs = in.readLong();
        int n = in.readInt();
        if (n < 0 || n > 1000) throw new BadRequest("topics");
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < n; i++) topics.add(str(in));
        var r = groups.join(group, member, topics, Math.max(1000, Math.min(sessionMs, 600_000)));
        reply(ctx, corr, r.error(), b -> {
            writeStr(b, r.memberId());
            b.writeInt(r.generation());
            b.writeInt(r.assignment().size());
            for (TopicPartition tp : r.assignment()) { writeStr(b, tp.topic()); b.writeInt(tp.partition()); }
        });
    }

    private void heartbeat(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        short err = groups.heartbeat(str(in), str(in), in.readInt());
        reply(ctx, corr, err, b -> {});
    }

    private void leave(ChannelHandlerContext ctx, int corr, ByteBuf in) {
        groups.leave(str(in), str(in));
        reply(ctx, corr, Errors.NONE, b -> {});
    }

    // ---- wire helpers --------------------------------------------------------------------------------

    private static void reply(ChannelHandlerContext ctx, int corr, short err, java.util.function.Consumer<ByteBuf> body) {
        ByteBuf b = ctx.alloc().buffer();
        b.writeInt(0).writeInt(corr).writeShort(err);
        body.accept(b);
        b.setInt(0, b.writerIndex() - 4);
        ctx.writeAndFlush(b);
    }

    private static String str(ByteBuf b) {
        int len = b.readUnsignedShort();
        if (len > b.readableBytes()) throw new BadRequest("string");
        return b.readCharSequence(len, StandardCharsets.UTF_8).toString();
    }

    private static void writeStr(ByteBuf b, String s) {
        byte[] x = s.getBytes(StandardCharsets.UTF_8);
        b.writeShort(x.length).writeBytes(x);
    }

    private static byte[] bytes(ByteBuf b) {
        int len = b.readInt();
        if (len == -1) return null;
        if (len < 0 || len > b.readableBytes()) throw new BadRequest("bytes");
        byte[] x = new byte[len];
        b.readBytes(x);
        return x;
    }
}
