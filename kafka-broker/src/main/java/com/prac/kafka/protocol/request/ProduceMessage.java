package com.prac.kafka.protocol.request;

public record ProduceMessage(String key, String value) {
}
