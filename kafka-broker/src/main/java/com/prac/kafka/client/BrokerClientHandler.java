package com.prac.kafka.client;

import com.prac.kafka.common.model.Packet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.CompletableFuture;

public class BrokerClientHandler extends SimpleChannelInboundHandler<Packet> {

    private CompletableFuture<Packet> responseFuture;

    public void setResponseFuture(CompletableFuture<Packet> future) {
        this.responseFuture = future;
    }

    public CompletableFuture<Packet> getResponseFuture() {
        return responseFuture;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) throws Exception {
        responseFuture.complete(msg);
    }
}
