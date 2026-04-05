package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.consumer.OffsetManager;
import com.prac.kafka.protocol.request.CommitOffsetRequest;
import com.prac.kafka.protocol.response.CommitOffsetResponse;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitOffsetHandler {

    private static final Logger log = LoggerFactory.getLogger(CommitOffsetHandler.class);

    private final OffsetManager offsetManager;
    private final ObjectMapper objectMapper;

    public CommitOffsetHandler(OffsetManager offsetManager, ObjectMapper objectMapper) {
        this.offsetManager = offsetManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received COMMIT_OFFSET packet. channel={}", ctx.channel().id());

        try {
            CommitOffsetRequest offsetRequest = objectMapper.readValue(packet.payload(), CommitOffsetRequest.class);

            offsetManager.commit(offsetRequest.consumerId(), offsetRequest.topic(), offsetRequest.offset());

            CommitOffsetResponse commitOffsetResponse = new CommitOffsetResponse(offsetRequest.consumerId(), offsetRequest.topic(), offsetRequest.offset());
            Packet response = new Packet(Command.COMMIT_ACK, objectMapper.writeValueAsBytes(commitOffsetResponse));
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            log.error("Unexpected error while handling COMMIT_OFFSET. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "INTERNAL_ERROR", e.getMessage());
        }
    }
}
