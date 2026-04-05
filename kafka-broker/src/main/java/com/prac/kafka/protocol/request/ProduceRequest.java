package com.prac.kafka.protocol.request;

public record ProduceRequest(String topic, String key, String value) {
}
