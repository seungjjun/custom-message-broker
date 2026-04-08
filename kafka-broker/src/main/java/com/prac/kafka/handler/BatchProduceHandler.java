package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.protocol.request.BatchProduceRequest;
import com.prac.kafka.protocol.request.ProduceMessage;
import com.prac.kafka.protocol.response.BatchProduceResponse;
import com.prac.kafka.protocol.response.ProduceResult;
import com.prac.kafka.storage.Partition;
import com.prac.kafka.storage.PartitionedTopic;
import com.prac.kafka.storage.TopicManager;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchProduceHandler {

    private static final Logger log = LoggerFactory.getLogger(BatchProduceHandler.class);

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;

    public BatchProduceHandler(TopicManager topicManager, ObjectMapper objectMapper) {
        this.topicManager = topicManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received BATCH_PRODUCE packet. channel={}", ctx.channel().id());

        try {
            BatchProduceRequest batchProduceRequest = objectMapper.readValue(packet.payload(), BatchProduceRequest.class);

            PartitionedTopic topic = topicManager.getTopic(batchProduceRequest.topic());
            List<ProduceResult> results = new ArrayList<>();
            for (ProduceMessage message : batchProduceRequest.messages()) {
                int selectedPartition = topic.selectPartition(message.key());
                Partition partition = topic.getPartition(selectedPartition);
                long offset = partition.append(message.key(), message.value().getBytes(StandardCharsets.UTF_8));

                log.info("Batch produce completed. topic={}, messageCount={}, channel={}",
                    batchProduceRequest.topic(), results.size(), ctx.channel().id());

                ProduceResult result = new ProduceResult(selectedPartition, offset);
                results.add(result);
            }

            BatchProduceResponse batchProduceResponse = new BatchProduceResponse(topic.getName(), results);
            Packet response = new Packet(Command.BATCH_PRODUCE_ACK, objectMapper.writeValueAsBytes(batchProduceResponse));
            ctx.writeAndFlush(response);
        } catch (IllegalArgumentException e) {
            log.warn("Bad batch produce request: {}", e.getMessage());
            ErrorResponseWriter.writeError(ctx, objectMapper, "BAD_REQUEST", e.getMessage());
        }
        catch (Exception e) {
            log.error("Unexpected error while handling BATCH_PRODUCE. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "INTERNAL_ERROR", e.getMessage());
        }
    }
}
