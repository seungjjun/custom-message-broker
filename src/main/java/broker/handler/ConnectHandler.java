package broker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Map;
import common.model.Command;
import common.model.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.util.JsonUtil;

public class ConnectHandler extends SimpleChannelInboundHandler<Packet> {

    private static final Logger log = LoggerFactory.getLogger(ConnectHandler.class);

    @Override
    public boolean acceptInboundMessage(Object msg) {
        if (msg instanceof Packet p && p.command() == Command.CONNECT) {
            return true;
        }
        return false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        Map<?, ?> body = JsonUtil.fromBytes(packet.payload(), Map.class);
        String clientId = (String) body.get("clientId");

        // TODO clientId session 확인
        sendAck(ctx, 0); // 0: 성공, 1: 중복
        log.info("Client {} connected", clientId);
    }

    private void sendAck(ChannelHandlerContext ctx, int status) {
        byte[] payload = JsonUtil.toBytes(Map.of("status", status));
        Packet packet = new Packet(Command.CONNACK, payload);
        ctx.writeAndFlush(packet);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("CONNECT error", cause);
        ctx.close();
    }
}
