package com.prac.kafka.bootstrap;

import com.prac.kafka.handler.BrokerServerHandler;
import com.prac.kafka.protocol.MessageDecoder;
import com.prac.kafka.protocol.MessageEncoder;
import com.prac.kafka.protocol.MessageFrameDecoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class KafkaBrokerServer {

    private static final int MAX_FRAME_LENGTH = 1_048_576;

    private final int port;

    public KafkaBrokerServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        IoHandlerFactory ioHandlerFactory = NioIoHandler.newFactory();
        MultiThreadIoEventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, ioHandlerFactory);
        MultiThreadIoEventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(ioHandlerFactory);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(new MessageFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                        pipeline.addLast(new MessageDecoder());
                        pipeline.addLast(new MessageEncoder());
                        pipeline.addLast(new BrokerServerHandler());
                    }
                });

            ChannelFuture bindFuture = bootstrap.bind(port).sync();

            bindFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
