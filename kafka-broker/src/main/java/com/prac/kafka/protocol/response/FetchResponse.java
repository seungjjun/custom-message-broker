package com.prac.kafka.protocol.response;

import java.util.List;

public record FetchResponse(List<FetchRecord> records, long nextOffset) {
}
