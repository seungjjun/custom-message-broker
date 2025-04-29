import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

public class ChatServerHandler extends SimpleChannelInboundHandler<String> {

    private static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        Channel incoming = ctx.channel();

        channels.writeAndFlush("[SERVER] - " + incoming.remoteAddress() + " joined.\n");

        channels.add(incoming);
        System.out.println("[SERVER] 클라이언트 접속: " + incoming.remoteAddress());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        Channel incoming = ctx.channel();

        channels.writeAndFlush("[SERVER] - " + incoming.remoteAddress() + " left.\n");

        System.out.println("[SERVER] 클라이언트 퇴장: " + incoming.remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        Channel incoming = ctx.channel();

        for (Channel channel : channels) {
            if (channel != incoming) {
                channel.writeAndFlush("[" + incoming.remoteAddress() + "]" + msg + "\n");
            } else {
                channel.writeAndFlush("[You] " + msg + "\n");
            }
        }
        System.out.println("[" + incoming.remoteAddress() + "] " + msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("[SERVER] 예외 발생: " + ctx.channel().remoteAddress());
        cause.printStackTrace();
        ctx.close();
    }
}
