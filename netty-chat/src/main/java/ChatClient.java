import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ChatClient {

    private final String host;
    private final int port;

    public ChatClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws Exception {
        IoHandlerFactory ioHandlerFactory = NioIoHandler.newFactory();
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(ioHandlerFactory);

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline()
                            .addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()))
                            .addLast("decoder", new StringDecoder())
                            .addLast("encoder", new StringEncoder())
                            .addLast("handler", new ChatClientHandler());
                    }
                });

            System.out.println("서버 연결 시도: " + host + ":" + port);
            Channel channel = bootstrap.connect(host, port).sync().channel();

            System.out.println("서버에 연결되었습니다. 메시지를 입력하세요.");

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String line = in.readLine();
                if (line == null || "quit".equalsIgnoreCase(line)) {
                    break;
                }
                channel.writeAndFlush(line + "\r\n");
            }

            channel.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
            System.out.println("Client shut down gracefully.");
        }
    }
}
