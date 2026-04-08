package com.prac.kafka.protocol.response;

import java.util.List;

public record BatchProduceResponse(String topic, List<ProduceResult> results) {
}
