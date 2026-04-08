package com.prac.kafka.protocol.request;

import java.util.List;

public record BatchProduceRequest(String topic, List<ProduceMessage> messages) {
}
