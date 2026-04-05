package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.consumer.OffsetManager;
import com.prac.kafka.protocol.request.GetOffsetRequest;
import com.prac.kafka.protocol.response.GetOffsetResponse;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetOffsetHandler {

    private static final Logger log = LoggerFactory.getLogger(GetOffsetHandler.class);

    private final OffsetManager offsetManager;
    private final ObjectMapper objectMapper;

    public GetOffsetHandler(OffsetManager offsetManager, ObjectMapper objectMapper) {
        this.offsetManager = offsetManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received GET_OFFSET packet. channel={}", ctx.channel().id());

        try {
            GetOffsetRequest getOffsetRequest = objectMapper.readValue(packet.payload(), GetOffsetRequest.class);

            long committed = offsetManager.getCommitted(getOffsetRequest.consumerId(), getOffsetRequest.topic());

            GetOffsetResponse getOffsetResponse = new GetOffsetResponse(getOffsetRequest.consumerId(), getOffsetRequest.topic(), committed);
            Packet response = new Packet(Command.GET_OFFSET_RESPONSE, objectMapper.writeValueAsBytes(getOffsetResponse));
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            log.error("Unexpected error while handling GET_OFFSET. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "INTERNAL_ERROR", e.getMessage());
        }

    }
}
