package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.protocol.request.CreateTopicRequest;
import com.prac.kafka.protocol.response.CreateTopicResponse;
import com.prac.kafka.storage.TopicManager;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTopicHandler {

    private static final Logger log = LoggerFactory.getLogger(CreateTopicHandler.class);

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;

    public CreateTopicHandler(TopicManager topicManager, ObjectMapper objectMapper) {
        this.topicManager = topicManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received CREATE_TOPIC packet. channel={}", ctx.channel().id());

        try {
            CreateTopicRequest createTopicRequest = objectMapper.readValue(packet.payload(), CreateTopicRequest.class);

            log.info("Handling create topic request. topic={}", createTopicRequest);

            topicManager.createTopic(createTopicRequest.topic(), createTopicRequest.partitions());

            log.info("Topic created. topic={}, partitions={}, channel={}", createTopicRequest.topic(), createTopicRequest.partitions(), ctx.channel().id());

            CreateTopicResponse createTopicResponse = new CreateTopicResponse(createTopicRequest.topic(), createTopicRequest.partitions());
            Packet response = new Packet(Command.TOPIC_ACK, objectMapper.writeValueAsBytes(createTopicResponse));
            ctx.writeAndFlush(response);
        } catch (IllegalArgumentException e) {
            log.warn("Create topic failed. channel={}, reason={}", ctx.channel().id(), e.getMessage(), e);

            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while handling CREATE_TOPIC. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        }
    }
}
