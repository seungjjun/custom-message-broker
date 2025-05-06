package broker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import common.model.Command;
import common.model.Packet;

public class DisconnectHandler extends SimpleChannelInboundHandler<Packet> {

    @Override
    public boolean acceptInboundMessage(Object msg) {
        return msg instanceof Packet p && p.command() == Command.DISCONNECT;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        String channelId = ctx.channel().id().asShortText();
        // TODO 세션 스토어에서 제거
        ctx.close();
    }
}
