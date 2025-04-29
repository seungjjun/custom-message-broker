import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class ChatServer {

    private final int port;

    public ChatServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        IoHandlerFactory ioHandlerFactory = NioIoHandler.newFactory();
        MultiThreadIoEventLoopGroup mainGroup = new MultiThreadIoEventLoopGroup(ioHandlerFactory);
        MultiThreadIoEventLoopGroup subGroup = new MultiThreadIoEventLoopGroup(ioHandlerFactory);

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(mainGroup, subGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter())) // 줄바꿈 기준으로 메시지 구분
                            .addLast("decoder", new StringDecoder())
                            .addLast("encoder", new StringEncoder())
                            .addLast("handler", new ChatServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128) // 대기 큐 사이즈 설정
                .childOption(ChannelOption.SO_KEEPALIVE, true); // TCP keep-alive 활성화

            System.out.println("Netty 채팅 서버 시작. 포트: " + port);

            ChannelFuture future = serverBootstrap.bind(port).sync();

            // 서버 소켓 채널이 닫힐 떄까지 대기
            future.channel().closeFuture().sync();

        } finally {
            mainGroup.shutdownGracefully();
            subGroup.shutdownGracefully();
            System.out.println("Server shut down gracefully.");
        }
    }

}
