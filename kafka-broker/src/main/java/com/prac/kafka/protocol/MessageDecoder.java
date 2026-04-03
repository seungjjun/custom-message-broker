package com.prac.kafka.protocol;

import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        byte opcode = msg.readByte();
        int readableBytes = msg.readableBytes();
        byte[] bytes = new byte[readableBytes];
        msg.readBytes(bytes);

        Command command = Command.fromOpcode(opcode);
        Packet packet = new Packet(command, bytes);

        out.add(packet);
    }
}
