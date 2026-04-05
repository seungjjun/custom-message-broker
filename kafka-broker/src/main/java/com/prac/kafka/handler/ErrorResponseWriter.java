package com.prac.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.protocol.response.ErrorResponse;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorResponseWriter {

    private static final Logger log = LoggerFactory.getLogger(ErrorResponseWriter.class);

    public static void writeError(ChannelHandlerContext ctx, ObjectMapper objectMapper, String code, String message) {
        try {
            ErrorResponse errorResponse = new ErrorResponse(code, message);
            byte[] payload = objectMapper.writeValueAsBytes(errorResponse);
            ctx.writeAndFlush(new Packet(Command.ERROR, payload));
        } catch (Exception e) {
            log.error("Failed to serialize error response. code={}, message={}", code, message, e);
            byte[] fallback = "{\"code\":\"INTERNAL_ERROR\",\"message\":\"Internal server error\"}"
                .getBytes(StandardCharsets.UTF_8);
            ctx.writeAndFlush(new Packet(Command.ERROR, fallback));
        }
    }
}
