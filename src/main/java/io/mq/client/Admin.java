package io.mq.client;

import io.mq.domain.Api;
import io.mq.domain.Errors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/** Topic administration and metadata. */
public final class Admin implements AutoCloseable {
    private final BrokerConnection conn;

    public Admin(String host, int port) { this.conn = new BrokerConnection(host, port); }

    /** Idempotent when the topic already exists with the same partition count. */
    public void createTopic(String topic, int partitions) {
        try {
            var r = conn.call(Api.CREATE_TOPIC, BrokerConnection.str(topic), (ByteBuffer) ByteBuffer.allocate(4).putInt(partitions).flip());
            if (r.error() != Errors.NONE) throw new ClientException(r.error(), "createTopic " + topic);
        } catch (IOException e) { throw new ClientException("createTopic failed", e); }
    }

    public Map<String, Integer> topics() {
        try {
            var r = conn.call(Api.METADATA, (ByteBuffer) ByteBuffer.allocate(4).putInt(0).flip());
            if (r.error() != Errors.NONE) throw new ClientException(r.error(), "metadata");
            Map<String, Integer> out = new TreeMap<>();
            int n = r.body().getInt();
            for (int i = 0; i < n; i++) out.put(BrokerConnection.getStr(r.body()), r.body().getInt());
            return out;
        } catch (IOException e) { throw new ClientException("metadata failed", e); }
    }

    @Override public void close() { conn.close(); }
}
