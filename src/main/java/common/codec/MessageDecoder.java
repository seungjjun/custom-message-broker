package common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import common.model.Command;
import common.model.Packet;

public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        byte opcode = in.readByte();
        byte[] payload = new byte[in.readableBytes()];
        in.readBytes(payload);

        out.add(new Packet(Command.fromOpcode(opcode), payload));
    }
}
