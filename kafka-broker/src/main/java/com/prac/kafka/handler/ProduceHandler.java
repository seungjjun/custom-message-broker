package com.prac.kafka.handler;

import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.protocol.request.ProduceRequest;
import com.prac.kafka.protocol.response.ProduceResponse;
import com.prac.kafka.storage.TopicLog;
import com.prac.kafka.storage.TopicManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceHandler {

    private static final Logger log = LoggerFactory.getLogger(ProduceHandler.class);

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;

    public ProduceHandler(TopicManager topicManager, ObjectMapper objectMapper) {
        this.topicManager = topicManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received PRODUCE packet. channel={}", ctx.channel().id());

        try {
            ProduceRequest produceRequest = objectMapper.readValue(packet.payload(), ProduceRequest.class);

            log.info("Handling produce request. topic={}, key={}", produceRequest.topic(), produceRequest.key());

            TopicLog topicLog = topicManager.getTopicLog(produceRequest.topic());
            long offset = topicLog.append(produceRequest.key(), produceRequest.value().getBytes(StandardCharsets.UTF_8));

            log.info("Produce succeeded. topic={}, key={}, offset={}, channel={}",
                produceRequest.topic(), produceRequest.key(), offset, ctx.channel().id());

            ProduceResponse produceResponse = new ProduceResponse(produceRequest.topic(), offset);
            Packet response = new Packet(Command.PRODUCE_ACK, objectMapper.writeValueAsBytes(produceResponse));
            ctx.writeAndFlush(response);
        } catch (IllegalArgumentException e) {
            log.warn("Produce failed due to invalid request or missing topic. channel={}, reason={}",
                ctx.channel().id(), e.getMessage(), e);

            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while handling PRODUCE. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        }
    }

}
