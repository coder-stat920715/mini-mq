package io.mq.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.Closeable;
import java.net.InetSocketAddress;

/** Non-blocking binary-protocol server (Netty). One event-loop thread per core handles all connections. */
public final class BrokerServer implements Closeable {
    private static final int MAX_FRAME = 64 * 1024 * 1024;

    private final RequestHandler handler;
    private final int port;
    private final int ioThreads;
    private EventLoopGroup boss, workers;
    private Channel server;

    public BrokerServer(RequestHandler handler, int port, int ioThreads) {
        this.handler = handler; this.port = port; this.ioThreads = ioThreads;
    }

    public synchronized void start() throws InterruptedException {
        boss = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup(ioThreads);
        server = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(512 * 1024, 4 * 1024 * 1024))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                          .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME, 0, 4, 0, 4))   // strips the length prefix
                          .addLast(new Frames(handler));
                    }
                })
                .bind(port).sync().channel();
    }

    public int port() { return ((InetSocketAddress) server.localAddress()).getPort(); }

    @Override public synchronized void close() {
        if (server != null) server.close().syncUninterruptibly();
        if (workers != null) workers.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
        if (boss != null) boss.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
    }

    private static final class Frames extends ChannelInboundHandlerAdapter {
        private final RequestHandler handler;
        Frames(RequestHandler h) { this.handler = h; }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf frame = (ByteBuf) msg;
            try { handler.handle(ctx, frame); } finally { frame.release(); }
        }
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
    }
}
