package com.prac.kafka.common.model;

public record Packet(Command command, byte[] payload) {
}
