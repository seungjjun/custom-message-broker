package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.common.model.Record;
import com.prac.kafka.protocol.request.FetchRequest;
import com.prac.kafka.protocol.response.FetchRecord;
import com.prac.kafka.protocol.response.FetchResponse;
import com.prac.kafka.storage.TopicLog;
import com.prac.kafka.storage.TopicManager;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchHandler {

    private static final Logger log = LoggerFactory.getLogger(FetchHandler.class);

    private final TopicManager topicManager;
    private final ObjectMapper objectMapper;

    public FetchHandler(TopicManager topicManager, ObjectMapper objectMapper) {
        this.topicManager = topicManager;
        this.objectMapper = objectMapper;
    }

    public void handle(ChannelHandlerContext ctx, Packet packet) {
        log.info("Received FETCH packet. channel={}", ctx.channel().id());

        try {
            FetchRequest fetchRequest = objectMapper.readValue(packet.payload(), FetchRequest.class);

            log.info("Handling fetch request. topic={}, offset={}, maxRecords={}",
                fetchRequest.topic(), fetchRequest.offset(), fetchRequest.maxRecords());

            TopicLog topicLog = topicManager.getTopicLog(fetchRequest.topic());
            List<Record> records = topicLog.fetch(fetchRequest.offset(), fetchRequest.maxRecords());
            List<FetchRecord> fetchRecords = records.stream().map(FetchRecord::from).toList();

            long nextOffset = fetchRecords.isEmpty()
                ? fetchRequest.offset()
                : records.getLast().offset() + 1;
            FetchResponse fetchResponse = new FetchResponse(fetchRecords, nextOffset);
            Packet response = new Packet(Command.FETCH_RESPONSE, objectMapper.writeValueAsBytes(fetchResponse));
            ctx.writeAndFlush(response);
        } catch (IllegalArgumentException e) {
            log.warn("Fetch failed due to invalid request or missing topic. channel={}, reason={}",
                ctx.channel().id(), e.getMessage(), e);

            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while handling FETCH. channel={}", ctx.channel().id(), e);
            ErrorResponseWriter.writeError(ctx, objectMapper, "TEMP", e.getMessage());
        }
    }
}
