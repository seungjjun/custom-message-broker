package broker.server;

import common.codec.MessageDecoder;
import common.codec.MessageEncoder;
import common.codec.MessageFrameDecoder;
import broker.handler.ConnectHandler;
import broker.handler.DisconnectHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerServer {

    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);
    private final int port;

    public BrokerServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        IoHandlerFactory ioHandlerFactory = NioIoHandler.newFactory();
        MultiThreadIoEventLoopGroup boss = new MultiThreadIoEventLoopGroup(ioHandlerFactory);
        MultiThreadIoEventLoopGroup worker = new MultiThreadIoEventLoopGroup(ioHandlerFactory);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new MessageFrameDecoder());
                        p.addLast(new MessageDecoder());
                        p.addLast(new MessageEncoder());
                        p.addLast(new ConnectHandler());
                        p.addLast(new DisconnectHandler());
                    }
                });

            ChannelFuture f = bootstrap.bind(port).sync();
            log.info("Broker server started on port -> 0.0.0.0:{}", port);
            f.channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new BrokerServer(1883).start();
    }
}
