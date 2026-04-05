package com.prac.kafka.protocol.response;

public record ProduceResponse(String topic, long offset) { // TODO partition
}
