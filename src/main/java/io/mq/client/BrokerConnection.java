package io.mq.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/** Blocking, reconnecting request/response channel to one broker. Direct buffers go straight to the socket. */
public final class BrokerConnection implements Closeable {

    public record Response(short error, ByteBuffer body) {}

    private final String host;
    private final int port;
    private SocketChannel ch;
    private int correlation;

    public BrokerConnection(String host, int port) { this.host = host; this.port = port; }

    public synchronized Response call(short api, ByteBuffer... parts) throws IOException {
        int corr = write(api, parts);
        ByteBuffer len = ByteBuffer.allocate(4);
        try {
            readFully(len);
            len.flip();
            int n = len.getInt();
            if (n < 6 || n > 256 * 1024 * 1024) throw new IOException("bad response length " + n);
            ByteBuffer body = ByteBuffer.allocate(n);
            readFully(body);
            body.flip();
            if (body.getInt() != corr) throw new IOException("correlation id mismatch");
            short err = body.getShort();
            return new Response(err, body);
        } catch (IOException e) { closeQuietly(); throw e; }
    }

    /** Fire-and-forget (acks=0). */
    public synchronized void send(short api, ByteBuffer... parts) throws IOException { write(api, parts); }

    private int write(short api, ByteBuffer[] parts) throws IOException {
        if (ch == null || !ch.isOpen()) {
            ch = SocketChannel.open();
            ch.socket().setTcpNoDelay(true);
            ch.socket().connect(new InetSocketAddress(host, port), 5000);
        }
        int corr = ++correlation;
        long total = 6;
        for (ByteBuffer p : parts) total += p.remaining();
        ByteBuffer[] all = new ByteBuffer[parts.length + 1];
        ByteBuffer head = ByteBuffer.allocate(10);
        head.putInt((int) total).putShort(api).putInt(corr).flip();
        all[0] = head;
        System.arraycopy(parts, 0, all, 1, parts.length);
        try {
            while (hasRemaining(all)) ch.write(all);
        } catch (IOException e) { closeQuietly(); throw e; }
        return corr;
    }

    private static boolean hasRemaining(ByteBuffer[] bs) {
        for (ByteBuffer b : bs) if (b.hasRemaining()) return true;
        return false;
    }

    private void readFully(ByteBuffer b) throws IOException {
        while (b.hasRemaining()) if (ch.read(b) < 0) throw new IOException("connection closed by broker");
    }

    private void closeQuietly() {
        try { if (ch != null) ch.close(); } catch (IOException ignored) { }
        ch = null;
    }

    @Override public synchronized void close() { closeQuietly(); }

    // ---- small encoding helpers shared by the clients -------------------------------------------------

    static ByteBuffer str(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + b.length).putShort((short) b.length).put(b).flip();
    }

    static String getStr(ByteBuffer b) {
        byte[] x = new byte[b.getShort() & 0xFFFF];
        b.get(x);
        return new String(x, StandardCharsets.UTF_8);
    }

    static void putStr(ByteBuffer b, String s) {
        byte[] x = s.getBytes(StandardCharsets.UTF_8);
        b.putShort((short) x.length).put(x);
    }

    static int strSize(String s) { return 2 + s.getBytes(StandardCharsets.UTF_8).length; }
}
