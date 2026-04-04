package com.prac.kafka.common.model;

import java.util.Arrays;

public record Record(long offset, long timestamp, String key, byte[] value) {

    public Record {
        value = Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }
}
