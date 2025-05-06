package common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import common.model.Packet;

public class MessageEncoder extends MessageToByteEncoder<Packet> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf out) throws Exception {
        byte[] payload = packet.payload();
        int length = 1 + payload.length; // opcode + payload length

        out.writeInt(length);
        out.writeByte(packet.command().getOpcode());
        out.writeBytes(payload);
    }
}
