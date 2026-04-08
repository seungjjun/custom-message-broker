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

    private final ProduceHandler produceHandler;
    private final FetchHandler fetchHandler;
    private final CreateTopicHandler createTopicHandler;
    private final CommitOffsetHandler commitOffsetHandler;
    private final GetOffsetHandler getOffsetHandler;
    private final BatchProduceHandler batchProduceHandler;

    public BrokerServerHandler(ProduceHandler produceHandler,
                               FetchHandler fetchHandler,
                               CreateTopicHandler createTopicHandler,
                               CommitOffsetHandler commitOffsetHandler,
                               GetOffsetHandler getOffsetHandler,
                               BatchProduceHandler batchProduceHandler) {
        this.produceHandler = produceHandler;
        this.fetchHandler = fetchHandler;
        this.createTopicHandler = createTopicHandler;
        this.commitOffsetHandler = commitOffsetHandler;
        this.getOffsetHandler = getOffsetHandler;
        this.batchProduceHandler = batchProduceHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Client connected: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        Command command = packet.command();

        switch (command) {
            case PRODUCE -> produceHandler.handle(ctx, packet);
            case FETCH -> fetchHandler.handle(ctx, packet);
            case CREATE_TOPIC -> createTopicHandler.handle(ctx, packet);
            case COMMIT_OFFSET -> commitOffsetHandler.handle(ctx, packet);
            case GET_OFFSET -> getOffsetHandler.handle(ctx, packet);
            case BATCH_PRODUCE -> batchProduceHandler.handle(ctx, packet);
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

    private void handleUnknown(ChannelHandlerContext ctx, Packet packet) {
        log.error("Received unknown packet. command={}", packet.command());

        Packet errorResponse = new Packet(Command.ERROR, "UNKNOWN_COMMAND".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(errorResponse);
    }
}
