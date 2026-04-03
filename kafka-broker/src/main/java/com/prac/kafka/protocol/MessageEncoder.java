package com.prac.kafka.protocol;

import com.prac.kafka.common.model.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder extends MessageToByteEncoder<Packet> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet packet, ByteBuf out) throws Exception {
        byte[] payload = normalizePayload(packet.payload());
        int length = payload.length + 1; // payload.length + command.length

        out.writeInt(length);
        out.writeByte(packet.command().getOpcode());
        out.writeBytes(payload);
    }

    private byte[] normalizePayload(byte[] payload) {
        return payload == null ? new byte[0] : payload;
    }
}
