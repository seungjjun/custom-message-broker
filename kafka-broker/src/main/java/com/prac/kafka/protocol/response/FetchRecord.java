package com.prac.kafka.protocol.response;

import com.prac.kafka.common.model.Record;
import java.nio.charset.StandardCharsets;

public record FetchRecord(long offset, long timestamp, String key, String value) {

    public static FetchRecord from(Record record) {
        return new FetchRecord(record.offset(), record.timestamp(), record.key(), new String(record.value(), StandardCharsets.UTF_8));
    }
}
