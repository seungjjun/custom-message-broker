package com.prac.kafka.bootstrap;

import com.prac.kafka.handler.BatchProduceHandler;
import com.prac.kafka.handler.BrokerServerHandler;
import com.prac.kafka.handler.CommitOffsetHandler;
import com.prac.kafka.handler.CreateTopicHandler;
import com.prac.kafka.handler.FetchHandler;
import com.prac.kafka.handler.GetOffsetHandler;
import com.prac.kafka.handler.ProduceHandler;
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

    private final ProduceHandler produceHandler;
    private final FetchHandler fetchHandler;
    private final CreateTopicHandler createTopicHandler;
    private final CommitOffsetHandler commitOffsetHandler;
    private final GetOffsetHandler getOffsetHandler;
    private final BatchProduceHandler batchProduceHandler;
    private final int port;

    public KafkaBrokerServer(int port,
                             ProduceHandler produceHandler,
                             FetchHandler fetchHandler,
                             CreateTopicHandler createTopicHandler,
                             CommitOffsetHandler commitOffsetHandler,
                             GetOffsetHandler getOffsetHandler,
                             BatchProduceHandler batchProduceHandler) {
        this.port = port;
        this.produceHandler = produceHandler;
        this.fetchHandler = fetchHandler;
        this.createTopicHandler = createTopicHandler;
        this.commitOffsetHandler = commitOffsetHandler;
        this.getOffsetHandler = getOffsetHandler;
        this.batchProduceHandler = batchProduceHandler;
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
                        pipeline.addLast(new BrokerServerHandler(
                            produceHandler,
                            fetchHandler,
                            createTopicHandler,
                            commitOffsetHandler,
                            getOffsetHandler,
                            batchProduceHandler
                        ));
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
