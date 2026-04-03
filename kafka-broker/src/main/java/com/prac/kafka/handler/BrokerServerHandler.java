package com.prac.kafka.handler;

import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerServerHandler extends SimpleChannelInboundHandler<Packet> {

    private static final Logger log = LoggerFactory.getLogger(BrokerServerHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Client connected: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        Command command = packet.command();

        switch (command) {
            case PRODUCE -> handleProduce(ctx, packet);
            case FETCH -> handleFetch(ctx, packet);
            default -> handleUnknown(ctx, packet);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Client disconnected: {}", ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught", cause);
        Packet errorPacket = new Packet(Command.ERROR, "UNKNOWN ERROR".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(errorPacket).addListener(future -> {
            ctx.close();
        });
    }

    private void handleProduce(ChannelHandlerContext ctx, Packet packet) {
        String payload = extractPayloadFrom(packet);
        log.info("Received PRODUCE packet. payload={}", payload);

        if (payload.isBlank()) {
            log.warn("PRODUCE payload is empty");
            Packet response = new Packet(Command.ERROR, "PRODUCE ERROR".getBytes(StandardCharsets.UTF_8));
            ctx.writeAndFlush(response);
            return;
        }

        Packet response = new Packet(Command.PRODUCE_ACK, "PRODUCE OK".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(response);
    }

    private void handleFetch(ChannelHandlerContext ctx, Packet packet) {
        String payload = extractPayloadFrom(packet);
        log.info("Received FETCH packet. payload={}", payload);

        if (payload.isBlank()) {
            log.warn("FETCH payload is empty");
            Packet response = new Packet(Command.ERROR, "FETCH ERROR".getBytes(StandardCharsets.UTF_8));
            ctx.writeAndFlush(response);
            return;
        }

        Packet response = new Packet(Command.FETCH_RESPONSE, "example response".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(response);
    }

    private void handleUnknown(ChannelHandlerContext ctx, Packet packet) {
        log.error("Received unknown packet. command={}", packet.command());

        Packet errorResponse = new Packet(Command.ERROR, "UNKNOWN_COMMAND".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(errorResponse);
    }

    private String extractPayloadFrom(Packet packet) {
        byte[] payload = packet.payload();
        if (payload == null || payload.length == 0) {
            return "";
        }
        return new String(payload, StandardCharsets.UTF_8);
    }
}
